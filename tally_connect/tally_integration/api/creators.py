"""
Tally Master Creation APIs - Production Ready
Creates masters in Tally with comprehensive validation and error handling

Version: 2.0.0
Date: December 14, 2025, 2:30 AM IST

FEATURES:
- Multi-company support (Company.custom_tally_company_name)
- Uses ERPNext's GST validation (no reinventing wheel)
- Item Name as Stock Item, Item Code as Alias
- Alternate unit support (Box/Pcs conversion)
- Proper error handling with retry job creation
- Comprehensive logging
"""

import frappe
from frappe import _
from frappe.utils import flt, cint, now
from tally_connect.tally_integration.utils import (
    get_settings,
    escape_xml,
    create_sync_log,
    send_xml_to_tally,
    check_master_exists,
    get_tally_company_name,
    format_date_for_tally,
    format_amount_for_tally,
    get_address_from_gstin
)


# ============================================================================
# ⭐ NEW: Wrapper function called from approval workflow
# ============================================================================

def create_master_from_request(request_doc):
    """
    Main entry point from approval workflow
    Routes to appropriate creator based on master_type
    
    Args:
        request_doc: Tally Master Creation Request document
    
    Returns:
        dict: {success: bool, sync_log: str, error: str}
    """
    
    # Map master type to creator function
    creator_map = {
        "Customer": create_customer_ledger_in_tally,
        "Supplier": create_supplier_ledger_in_tally,
        "Item": create_stock_item_in_tally,
        "Group": create_group_in_tally,
        "Stock Group": create_stock_group_in_tally,
        "Unit": create_unit_in_tally,
        "Godown": create_godown_in_tally
    }
    
    creator_func = creator_map.get(request_doc.master_type)
    
    if not creator_func:
        return {
            "success": False,
            "error": f"Unsupported master type: {request_doc.master_type}"
        }
    
    try:
        # Call appropriate creator with request context
        result = creator_func(
            doc_name=request_doc.erpnext_document,
            company=request_doc.company,
            request_doc=request_doc  # ⭐ NEW: Pass request for tracking
        )
        
        return result
        
    except Exception as e:
        import traceback
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        
        frappe.log_error(
            message=f"Master creation failed: {error_msg}\n\n{stack_trace}",
            title=f"Tally Creator Error: {request_doc.name}"
        )
        
        return {
            "success": False,
            "error": error_msg,
            "stack_trace": stack_trace
        }

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_tally_company_for_erpnext_company(company_name):
    """
    Get Tally company name for given ERPNext company
    Priority: Company.custom_tally_company_name > Settings.tally_company_name
    
    Args:
        company_name: ERPNext company name
    
    Returns:
        str: Tally company name
    """
    if not company_name:
        settings = get_settings()
        return settings.tally_company_name or ""
    
    # Try to get from Company custom field
    try:
        company = frappe.get_doc("Company", company_name)
        if hasattr(company, 'custom_tally_company_name') and company.custom_tally_company_name:
            return company.custom_tally_company_name
    except:
        pass
    
    # Fallback to settings
    settings = get_settings()
    return settings.tally_company_name or ""


def create_retry_job(document_type, document_name, operation, error_message, max_retries=3):
    """
    Create retry job for failed sync
    
    Args:
        document_type: "Customer", "Item", etc.
        document_name: Document ID
        operation: "Create Ledger", "Create Stock Item", etc.
        error_message: Error description
        max_retries: Maximum retry attempts (default: 3)
    
    Returns:
        Tally Retry Job document
    """
    try:
        retry_job = frappe.new_doc("Tally Retry Job")
        retry_job.document_type = document_type
        retry_job.document_name = document_name
        retry_job.operation = operation
        retry_job.retry_count = 0
        retry_job.max_retries = max_retries
        retry_job.status = "Pending"
        retry_job.error_message = error_message[:500]
        retry_job.next_retry_time = frappe.utils.add_to_date(now(), minutes=5)
        retry_job.insert(ignore_permissions=True)
        frappe.db.commit()
        return retry_job
    except Exception as e:
        frappe.log_error(f"Failed to create retry job: {str(e)}", "Tally Creators")
        return None


def get_customer_parent_group(customer_name, company):
    """
    Get parent ledger group for customer based on company's default account
    Uses: Company > Accounts > Default Receivable Account > Account Name
    
    Args:
        customer_name: Customer name
        customer_doc: Customer document (optional)
        company: Company name
    
    Returns:
        str: Parent group name (e.g., "Sundry Debtors")
    """
    try:
        # Get customer document
        customer = frappe.get_doc("Customer", customer_name)
        
        # Find default receivable account for this company
        for account in customer.accounts:
            if account.company == company:
                if account.account:
                    # Get account document to find parent
                    account_doc = frappe.get_doc("Account", account.account)
                    # Return parent account name (this is the group in Tally)
                    if account_doc.parent_account:
                        parent_doc = frappe.get_doc("Account", account_doc.parent_account)
                        return parent_doc.account_name
                    return account_doc.account_name
        
        # Fallback to settings
        settings = get_settings()
        return settings.default_customer_ledger or "Sundry Debtors"
    
    except Exception as e:
        frappe.log_error(
            f"Error getting customer parent group for {customer_name}: {str(e)}", 
            "Tally Creators"
        )
        settings = get_settings()
        return settings.default_customer_ledger or "Sundry Debtors"


def get_supplier_parent_group(supplier_name, company):
    """
    Get parent ledger group for supplier based on company's default account
    
    Args:
        supplier_name: Supplier name
        company: Company name
    
    Returns:
        str: Parent group name (e.g., "Sundry Creditors")
    """
    try:
        supplier = frappe.get_doc("Supplier", supplier_name)
        
        # Find default payable account for this company
        for account in supplier.accounts:
            if account.company == company:
                if account.account:
                    account_doc = frappe.get_doc("Account", account.account)
                    if account_doc.parent_account:
                        parent_doc = frappe.get_doc("Account", account_doc.parent_account)
                        return parent_doc.account_name
                    return account_doc.account_name
        
        # Fallback
        settings = get_settings()
        return settings.default_supplier_ledger or "Sundry Creditors"
    
    except Exception as e:
        frappe.log_error(
            f"Error getting supplier parent group for {supplier_name}: {str(e)}", 
            "Tally Creators"
        )
        settings = get_settings()
        return settings.default_supplier_ledger or "Sundry Creditors"


# ============================================================================
# GROUP CREATOR
# ============================================================================

@frappe.whitelist()
def create_group_in_tally(group_name, parent_group, company=None, is_revenue=False):
    """
    Create an account group in Tally
    
    Args:
        group_name: Name of the group to create
        parent_group: Parent group name (must exist in Tally)
        company: ERPNext company name (optional)
        is_revenue: Is this a revenue group? (for P&L)
    
    Returns:
        dict: {
            "success": bool,
            "message": str,
            "sync_log": str (if created),
            "already_exists": bool,
            "retry_job": str (if failed)
        }
    
    Example:
        create_group_in_tally("North Zone Debtors", "Sundry Debtors", "Your Company")
    """
    
    # Get Tally company name
    tally_company = get_tally_company_for_erpnext_company(company)
    
    # Validate parent group exists
    parent_check = check_master_exists("Group", parent_group)
    if not parent_check.get("exists"):
        error_msg = f"Parent group '{parent_group}' does not exist in Tally. Create it first."
        frappe.log_error(error_msg, "Tally Group Creator")
        
        # Create retry job
        retry_job = create_retry_job(
            document_type="Tally Group",
            document_name=group_name,
            operation="Create Group",
            error_message=error_msg
        )
        
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None
        }
    
    # Check if group already exists
    exists_check = check_master_exists("Group", group_name)
    if exists_check.get("exists"):
        return {
            "success": False,
            "error": f"Group '{group_name}' already exists in Tally",
            "already_exists": True,
            "action_required": "UPDATE"  # For future implementation
        }
    
    # Build Tally XML
    group_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Masters</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <GROUP NAME="{escape_xml(group_name)}" ACTION="Create">
          <NAME>{escape_xml(group_name)}</NAME>
          <PARENT>{escape_xml(parent_group)}</PARENT>
          <ISSUBLEDGER>No</ISSUBLEDGER>
          <ISBILLWISEON>No</ISBILLWISEON>
          <ISADDABLE>No</ISADDABLE>
          <ISREVENUE>{'Yes' if is_revenue else 'No'}</ISREVENUE>
          <AFFECTSSTOCK>No</AFFECTSSTOCK>
        </GROUP>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""
    
    # Create sync log
    log = create_sync_log(
        operation_type="Create Group",
        doctype_name="Tally Group",
        doc_name=group_name,
        company=company or "",
        xml=group_xml
    )
    
    # Send to Tally
    result = send_xml_to_tally(log, group_xml)
    
    # Handle result
    if not result.get("success"):
        # Create retry job for network/temporary errors
        if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
            retry_job = create_retry_job(
                document_type="Tally Group",
                document_name=group_name,
                operation="Create Group",
                error_message=result.get("error", "Unknown error")
            )
            return {
                "success": False,
                "error": result.get("error"),
                "error_type": result.get("error_type"),
                "sync_log": log.name,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # Validation errors - don't retry
        return {
            "success": False,
            "error": result.get("error"),
            "error_type": result.get("error_type"),
            "sync_log": log.name
        }
    
    return {
        "success": True,
        "message": f"Group '{group_name}' created successfully in Tally",
        "sync_log": log.name
    }


# ============================================================================
# LEDGER CREATOR (CUSTOMER/SUPPLIER)
# ============================================================================

# @frappe.whitelist()
# def create_customer_ledger_in_tally(customer_name, company=None):
#     """
#     Create customer ledger in Tally from ERPNext Customer
#     Uses ERPNext's field structure and validation
    
#     Args:
#         customer_name: ERPNext Customer name
#         company: ERPNext company name (optional, uses first company if not provided)
    
#     Returns:
#         dict: {"success": bool, "message": str, "sync_log": str, "retry_job": str}
    
#     Example:
#         create_customer_ledger_in_tally("ACME Corporation", "Your Company Ltd")
#     """
    
#     try:
#         # Get customer document
#         customer = frappe.get_doc("Customer", customer_name)
        
#         # Determine company
#         if not company:
#             # Use first company from customer's accounts
#             if customer.accounts and len(customer.accounts) > 0:
#                 company = customer.accounts[0].company
#             else:
#                 # Use default company from settings
#                 company = frappe.defaults.get_global_default("company")
        
#         # Get Tally company name
#         tally_company = get_tally_company_for_erpnext_company(company)
        
#         # Get parent group
#         parent_group = get_customer_parent_group(customer_name, company)
        
#         # Validate parent group exists in Tally
#         # Auto-create parent group if missing
#         parentcheck = check_master_exists("Group", parent_group)
#         if not parentcheck.get("exists"):
#             # Determine ultimate parent for this group
#             settings = get_settings()
#             ultimate_parent = getattr(settings, "default_customer_ledger", None) or "Sundry Debtors"
            
#             # If parent_group IS the ultimate parent, we can't create it automatically
#             if parent_group == ultimate_parent:
#                 errormsg = f"Parent group '{parent_group}' does not exist in Tally and is configured as base group"
#                 frappe.log_error(errormsg, "Tally Ledger Creator")
#                 retry_job = create_retry_job(
#                     document_type="Customer",
#                     document_name=customer_name,
#                     operation="Create Ledger",
#                     error_message=errormsg
#                 )
#                 return {
#                     "success": False,
#                     "error": errormsg,
#                     "retry_job": retry_job.name if retry_job else None
#                 }
            
#             # Try to create the intermediate parent group
#             frappe.msgprint(f"Auto-creating missing parent group '{parent_group}' under '{ultimate_parent}'", 
#                             indicator="blue", title="Tally Group Creation")
            
#             group_res = create_group_in_tally(parent_group, ultimate_parent, company)
#             if not group_res.get("success"):
#                 errormsg = f"Could not create parent group '{parent_group}': {group_res.get('error')}"
#                 frappe.log_error(errormsg, "Tally Ledger Creator")
#                 return {
#                     "success": False,
#                     "error": errormsg,
#                     "retry_job": group_res.get("retry_job"),
#                 }
            
#             frappe.msgprint(f"Parent group '{parent_group}' created successfully", 
#                             indicator="green", title="Tally Group Created")

#         # Check if ledger already exists
#         exists_check = check_master_exists("Ledger", customer.customer_name)
#         if exists_check.get("exists"):
#             return {
#                 "success": False,
#                 "error": f"Ledger '{customer.customer_name}' already exists in Tally",
#                 "already_exists": True,
#                 "action_required": "UPDATE"
#             }
        
#         # Build address (from primary address)
#         address_xml = ""
#         if customer.customer_primary_address:
#             try:
#                 address_doc = frappe.get_doc("Address", customer.customer_primary_address)
#                 address_lines = []
#                 if address_doc.address_line1:
#                     address_lines.append(address_doc.address_line1)
#                 if address_doc.address_line2:
#                     address_lines.append(address_doc.address_line2)
#                 if address_doc.city:
#                     address_lines.append(address_doc.city)
#                 if address_doc.state:
#                     address_lines.append(address_doc.state)
#                 if address_doc.pincode:
#                     address_lines.append(address_doc.pincode)
                
#                 if address_lines:
#                     address_xml = f"""
#           <ADDRESS.LIST>
#             <ADDRESS>{escape_xml(", ".join(address_lines))}</ADDRESS>
#           </ADDRESS.LIST>"""
#             except:
#                 pass
        
#         # Build GSTIN (use ERPNext's validation - already validated)
#         gstin_xml = ""
#         if customer.gstin:
#             gstin_xml = f"""
#           <PARTYGSTIN.LIST>
#             <PARTYGSTIN>{customer.gstin}</PARTYGSTIN>
#           </PARTYGSTIN.LIST>"""
        
#         # Build contact details
#         contact_xml = ""
#         if customer.mobile_no or customer.email_id:
#             contact_lines = []
#             if customer.mobile_no:
#                 contact_lines.append(f"Mobile: {customer.mobile_no}")
#             if customer.email_id:
#                 contact_lines.append(f"Email: {customer.email_id}")
            
#             contact_xml = f"""
#           <LEDGERCONTACT>{escape_xml("; ".join(contact_lines))}</LEDGERCONTACT>"""
        
#         # Build Tally XML
#         ledger_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#   <HEADER>
#     <VERSION>1</VERSION>
#     <TALLYREQUEST>Import</TALLYREQUEST>
#     <TYPE>Data</TYPE>
#     <ID>All Masters</ID>
#   </HEADER>
#   <BODY>
#     <DESC>
#       <STATICVARIABLES>
#         <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
#       </STATICVARIABLES>
#     </DESC>
#     <DATA>
#       <TALLYMESSAGE>
#         <LEDGER NAME="{escape_xml(customer.customer_name)}" ACTION="Create">
#           <NAME>{escape_xml(customer.customer_name)}</NAME>
#           <PARENT>{escape_xml(parent_group)}</PARENT>
#           <ISBILLWISEON>Yes</ISBILLWISEON>
#           <AFFECTSSTOCK>No</AFFECTSSTOCK>{address_xml}{gstin_xml}{contact_xml}
#         </LEDGER>
#       </TALLYMESSAGE>
#     </DATA>
#   </BODY>
# </ENVELOPE>"""
        
#         # Create sync log
#         log = create_sync_log(
#             operation_type="Create Customer Ledger",
#             doctype_name="Customer",
#             doc_name=customer_name,
#             company=company,
#             xml=ledger_xml
#         )
        
#         # Send to Tally
#         result = send_xml_to_tally(log, ledger_xml)
        
#         # Handle result
#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Customer",
#                     document_name=customer_name,
#                     operation="Create Ledger",
#                     error_message=result.get("error", "Unknown error")
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None
#                 }
            
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name
#             }
        
#         # Update customer document
#         try:
#             customer.db_set("custom_tally_synced", 1, update_modified=False)
#             customer.db_set("custom_tally_sync_date", now(), update_modified=False)
#         except:
#             pass
        
#         return {
#             "success": True,
#             "message": f"Customer ledger '{customer.customer_name}' created in Tally",
#             "sync_log": log.name
#         }
    
#     except Exception as e:
#         error_msg = f"Exception creating customer ledger: {str(e)}"
#         frappe.log_error(error_msg, "Tally Ledger Creator")
        
#         retry_job = create_retry_job(
#             document_type="Customer",
#             document_name=customer_name,
#             operation="Create Ledger",
#             error_message=error_msg
#         )
        
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None
#         }

# @frappe.whitelist()
# def create_customer_ledger_in_tally(customer_name, company=None):
#     """
#     Create customer ledger in Tally from ERPNext Customer
#     Uses ERPNext's field structure and validation
    
#     Args:
#         customer_name: ERPNext Customer name
#         company: ERPNext company name (optional, uses first company if not provided)
    
#     Returns:
#         dict: {"success": bool, "message": str, "sync_log": str, "retry_job": str}
    
#     Example:
#         create_customer_ledger_in_tally("ACME Corporation", "Your Company Ltd")
#     """
    
#     try:
#         # Get customer document
#         customer = frappe.get_doc("Customer", customer_name)
        
#         # Determine company
#         if not company:
#             # Use first company from customer's accounts
#             if customer.accounts and len(customer.accounts) > 0:
#                 company = customer.accounts[0].company
#             else:
#                 # Use default company from settings
#                 company = frappe.defaults.get_global_default("company")
        
#         # Get Tally company name
#         tally_company = get_tally_company_for_erpnext_company(company)
        
#         # Get settings
#         settings = get_settings()
#         base_group = getattr(settings, "default_customer_ledger", None) or "Sundry Debtors"
        
#         # Get default account for this customer and company
#         default_account_id = None
#         for acc_row in customer.accounts:
#             if acc_row.company == company:
#                 default_account_id = acc_row.account
#                 break
        
#         if not default_account_id:
#             errormsg = f"No default account found for customer '{customer_name}' in company '{company}'"
#             frappe.log_error(errormsg, "Tally Ledger Creator")
#             return {
#                 "success": False,
#                 "error": errormsg
#             }
        
#         # Get account document
#         account_doc = frappe.get_doc("Account", default_account_id)
#         parent_group = account_doc.account_name  # e.g., "Blinkit"
        
#         # Get parent account if exists
#         erp_parent_name = None
#         if account_doc.parent_account:
#             try:
#                 erp_parent = frappe.get_doc("Account", account_doc.parent_account)
#                 erp_parent_name = erp_parent.account_name  # e.g., "Q-Commerce"
#             except Exception:
#                 erp_parent_name = None
        
#         # 1) Ensure ERP parent group (e.g., "Q-Commerce") exists in Tally
#         if erp_parent_name:
#             parent_check = check_master_exists("Group", erp_parent_name)
#             if not parent_check.get("exists"):
#                 # Create ERP parent under base_group (e.g., Sundry Debtors)
#                 frappe.msgprint(
#                     f"Auto-creating missing parent group '{erp_parent_name}' under '{base_group}'",
#                     indicator="blue",
#                     title="Tally Group Creation"
#                 )
                
#                 pg_res = create_group_in_tally(erp_parent_name, base_group, company)
#                 if not pg_res.get("success"):
#                     errormsg = f"Could not create parent group '{erp_parent_name}': {pg_res.get('error')}"
#                     frappe.log_error(errormsg, "Tally Ledger Creator")
#                     return {
#                         "success": False,
#                         "error": errormsg,
#                         "retry_job": pg_res.get("retry_job"),
#                     }
                
#                 frappe.msgprint(
#                     f"Parent group '{erp_parent_name}' created successfully",
#                     indicator="green",
#                     title="Tally Group Created"
#                 )
        
#         # 2) Ensure the default account group itself (e.g., "Blinkit") exists in Tally
#         default_group_check = check_master_exists("Group", parent_group)
#         if not default_group_check.get("exists"):
#             # Decide parent for this group in Tally
#             tally_parent_for_default = erp_parent_name if erp_parent_name else base_group
            
#             frappe.msgprint(
#                 f"Auto-creating missing default account group '{parent_group}' under '{tally_parent_for_default}'",
#                 indicator="blue",
#                 title="Tally Group Creation"
#             )
            
#             dg_res = create_group_in_tally(parent_group, tally_parent_for_default, company)
#             if not dg_res.get("success"):
#                 errormsg = f"Could not create default account group '{parent_group}': {dg_res.get('error')}"
#                 frappe.log_error(errormsg, "Tally Ledger Creator")
#                 return {
#                     "success": False,
#                     "error": errormsg,
#                     "retry_job": dg_res.get("retry_job"),
#                 }
            
#             frappe.msgprint(
#                 f"Default account group '{parent_group}' created successfully",
#                 indicator="green",
#                 title="Tally Group Created"
#             )

#         # Check if ledger already exists
#         exists_check = check_master_exists("Ledger", customer.customer_name)
#         if exists_check.get("exists"):
#             return {
#                 "success": False,
#                 "error": f"Ledger '{customer.customer_name}' already exists in Tally",
#                 "already_exists": True,
#                 "action_required": "UPDATE"
#             }
        
#         # Build address (from primary address)
#         # Build address (from primary address)
#         address_xml = ""
#         address_doc = None

#         # 1) Primary address
#         if customer.customer_primary_address:
#             try:
#                 address_doc = frappe.get_doc("Address", customer.customer_primary_address)
#             except Exception as e:
#                 frappe.log_error(f"Could not fetch primary address for {customer.name}: {str(e)}",
#                                 "Tally Ledger Address Fetch")

#         # Helper to fetch first linked address by type
#         def _get_first_linked_address(customer_name, addr_type):
#             links = frappe.get_all(
#                 "Dynamic Link",
#                 filters={
#                     "link_doctype": "Customer",
#                     "link_name": customer_name,
#                     "parenttype": "Address",
#                 },
#                 fields=["parent"],
#                 order_by="creation asc",
#             )
#             for row in links:
#                 addr = frappe.get_doc("Address", row.parent)
#                 if (addr.address_type or "").lower() == addr_type.lower():
#                     return addr
#             return None

#         # 2) First Billing address
#         if not address_doc:
#             try:
#                 address_doc = _get_first_linked_address(customer.name, "Billing")
#             except Exception as e:
#                 frappe.log_error(f"Could not fetch billing address for {customer.name}: {str(e)}",
#                                 "Tally Ledger Address Fetch")

#         # 3) First Shipping address
#         if not address_doc:
#             try:
#                 address_doc = _get_first_linked_address(customer.name, "Shipping")
#             except Exception as e:
#                 frappe.log_error(f"Could not fetch shipping address for {customer.name}: {str(e)}",
#                                 "Tally Ledger Address Fetch")

#         # 4) Fallback from GSTIN (India)
#         if not address_doc and customer.gstin:
#             try:
#                 # You need to implement this helper using your India compliance/GST API
#                 data = get_address_from_gstin(customer.gstin)
#                 # data should be a dict with keys: address_line1, address_line2, city, state, pincode
#                 class Dummy: pass
#                 address_doc = Dummy()
#                 address_doc.address_line1 = data.get("address_line1")
#                 address_doc.address_line2 = data.get("address_line2")
#                 address_doc.city = data.get("city")
#                 address_doc.state = data.get("state")
#                 address_doc.pincode = data.get("pincode")
#             except Exception as e:
#                 frappe.log_error(f"Could not fetch address from GSTIN {customer.gstin} for {customer.name}: {str(e)}",
#                                 "Tally Ledger Address Fetch")

#         # Build XML if we have anything
#         if address_doc:
#             address_lines = []
#             if getattr(address_doc, "address_line1", None):
#                 address_lines.append(address_doc.address_line1)
#             if getattr(address_doc, "address_line2", None):
#                 address_lines.append(address_doc.address_line2)
#             if getattr(address_doc, "city", None):
#                 address_lines.append(address_doc.city)
#             if getattr(address_doc, "state", None):
#                 address_lines.append(address_doc.state)
#             if getattr(address_doc, "pincode", None):
#                 address_lines.append(str(address_doc.pincode))

#             if address_lines:
#                 address_xml = f"""
#                 <ADDRESS.LIST>
#                     <ADDRESS>{escape_xml(", ".join(address_lines))}</ADDRESS>
#                 </ADDRESS.LIST>"""


        
#         # Build GSTIN (use ERPNext's validation - already validated)
#         gstin_xml = ""
#         if customer.gstin:
#             gstin_xml = f"""
#           <PARTYGSTIN.LIST>
#             <PARTYGSTIN>{customer.gstin}</PARTYGSTIN>
#           </PARTYGSTIN.LIST>"""
        
#         # Build contact details
#         contact_xml = ""
#         if customer.mobile_no or customer.email_id:
#             contact_lines = []
#             if customer.mobile_no:
#                 contact_lines.append(f"Mobile: {customer.mobile_no}")
#             if customer.email_id:
#                 contact_lines.append(f"Email: {customer.email_id}")
            
#             contact_xml = f"""
#           <LEDGERCONTACT>{escape_xml("; ".join(contact_lines))}</LEDGERCONTACT>"""
        
#         # Build Tally XML
#         ledger_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#   <HEADER>
#     <VERSION>1</VERSION>
#     <TALLYREQUEST>Import</TALLYREQUEST>
#     <TYPE>Data</TYPE>
#     <ID>All Masters</ID>
#   </HEADER>
#   <BODY>
#     <DESC>
#       <STATICVARIABLES>
#         <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
#       </STATICVARIABLES>
#     </DESC>
#     <DATA>
#       <TALLYMESSAGE>
#         <LEDGER NAME="{escape_xml(customer.customer_name)}" ACTION="Create">
#           <NAME>{escape_xml(customer.customer_name)}</NAME>
#           <PARENT>{escape_xml(parent_group)}</PARENT>
#           <ISBILLWISEON>Yes</ISBILLWISEON>
#           <AFFECTSSTOCK>No</AFFECTSSTOCK>{address_xml}{gstin_xml}{contact_xml}
#         </LEDGER>
#       </TALLYMESSAGE>
#     </DATA>
#   </BODY>
# </ENVELOPE>"""
        
#         # Create sync log
#         log = create_sync_log(
#             operation_type="Create Customer Ledger",
#             doctype_name="Customer",
#             doc_name=customer_name,
#             company=company,
#             xml=ledger_xml
#         )
        
#         # Send to Tally
#         result = send_xml_to_tally(log, ledger_xml)
        
#         # Handle result
#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Customer",
#                     document_name=customer_name,
#                     operation="Create Ledger",
#                     error_message=result.get("error", "Unknown error")
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None
#                 }
            
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name
#             }
        
#         # Update customer document
#         try:
#             customer.db_set("custom_tally_synced", 1, update_modified=False)
#             customer.db_set("custom_tally_sync_date", now(), update_modified=False)
#         except:
#             pass
        
#         return {
#             "success": True,
#             "message": f"Customer ledger '{customer.customer_name}' created in Tally",
#             "sync_log": log.name
#         }
    
#     except Exception as e:
#         error_msg = f"Exception creating customer ledger: {str(e)}"
#         frappe.log_error(error_msg, "Tally Ledger Creator")
        
#         retry_job = create_retry_job(
#             document_type="Customer",
#             document_name=customer_name,
#             operation="Create Ledger",
#             error_message=error_msg
#         )
        
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None
#         }

@frappe.whitelist()
def create_customer_ledger_in_tally(customer_name, company=None):
    """
    Create customer ledger in Tally from ERPNext Customer

    Args:
        customer_name: ERPNext Customer name
        company: ERPNext company name (optional, uses first company if not provided)

    Returns:
        dict: {"success": bool, "message": str, "sync_log": str, "retry_job": str}
    """
    try:
        # Get customer document
        customer = frappe.get_doc("Customer", customer_name)

        # Determine company
        if not company:
            if customer.accounts and len(customer.accounts) > 0:
                company = customer.accounts[0].company
            else:
                company = frappe.defaults.get_global_default("company")

        # Get Tally company name (kept for future; not used directly in XML here)
        tally_company = get_tally_company_for_erpnext_company(company)

        # Settings and base group
        settings = get_settings()
        base_group = getattr(settings, "default_customer_ledger", None) or "Sundry Debtors"

        # ---------------- PARENT GROUP / HIERARCHY ----------------
        default_account_id = None
        for acc_row in customer.accounts:
            if acc_row.company == company and acc_row.account:
                default_account_id = acc_row.account
                break

        erp_parent_name = None  # e.g. Q‑Commerce
        if not default_account_id:
            # No mapping → put directly under base group
            parent_group = base_group
        else:
            account_doc = frappe.get_doc("Account", default_account_id)
            # Use account.account_name as group (e.g. Blinkit)
            parent_group = account_doc.account_name

            if account_doc.parent_account:
                try:
                    erp_parent = frappe.get_doc("Account", account_doc.parent_account)
                    erp_parent_name = erp_parent.account_name
                except Exception:
                    erp_parent_name = None

        # Ensure ERP parent group (Q‑Commerce) exists in Tally
        if erp_parent_name:
            parent_check = check_master_exists("Group", erp_parent_name)
            if not parent_check.get("exists"):
                frappe.msgprint(
                    f"Auto-creating missing parent group '{erp_parent_name}' under '{base_group}'",
                    indicator="blue",
                    title="Tally Group Creation",
                )
                pg_res = create_group_in_tally(erp_parent_name, base_group, company)
                if not pg_res.get("success"):
                    errormsg = f"Could not create parent group '{erp_parent_name}': {pg_res.get('error')}"
                    frappe.log_error(errormsg, "Tally Ledger Creator")
                    return {
                        "success": False,
                        "error": errormsg,
                        "retry_job": pg_res.get("retry_job"),
                    }
                frappe.msgprint(
                    f"Parent group '{erp_parent_name}' created successfully",
                    indicator="green",
                    title="Tally Group Created",
                )

        # Ensure default account group (Blinkit) exists
        if parent_group != base_group:
            default_group_check = check_master_exists("Group", parent_group)
            if not default_group_check.get("exists"):
                tally_parent_for_default = erp_parent_name or base_group
                frappe.msgprint(
                    f"Auto-creating missing default account group '{parent_group}' under '{tally_parent_for_default}'",
                    indicator="blue",
                    title="Tally Group Creation",
                )
                dg_res = create_group_in_tally(parent_group, tally_parent_for_default, company)
                if not dg_res.get("success"):
                    errormsg = f"Could not create default account group '{parent_group}': {dg_res.get('error')}"
                    frappe.log_error(errormsg, "Tally Ledger Creator")
                    return {
                        "success": False,
                        "error": errormsg,
                        "retry_job": dg_res.get("retry_job"),
                    }
                frappe.msgprint(
                    f"Default account group '{parent_group}' created successfully",
                    indicator="green",
                    title="Tally Group Created",
                )

        # ---------------- EXISTING LEDGER CHECK ----------------
        exists_check = check_master_exists("Ledger", customer.customer_name)
        if exists_check.get("exists"):
            return {
                "success": False,
                "error": f"Ledger '{customer.customer_name}' already exists in Tally",
                "already_exists": True,
                "action_required": "UPDATE",
            }

        # ---------------- ADDRESS RESOLUTION ----------------
        address_doc = None

        # 1) Primary address
        if customer.customer_primary_address:
            try:
                address_doc = frappe.get_doc("Address", customer.customer_primary_address)
            except Exception as e:
                frappe.log_error(
                    f"Could not fetch primary address for {customer.name}: {str(e)}",
                    "Tally Ledger Address Fetch",
                )

        # Helper: first linked address by type
        def _get_first_linked_address(cust_name, addr_type):
            links = frappe.get_all(
                "Dynamic Link",
                filters={
                    "link_doctype": "Customer",
                    "link_name": cust_name,
                    "parenttype": "Address",
                },
                fields=["parent"],
                order_by="creation asc",
            )
            for row in links:
                addr = frappe.get_doc("Address", row.parent)
                if (addr.address_type or "").lower() == addr_type.lower():
                    return addr
            return None

        # 2) First Billing
        if not address_doc:
            try:
                address_doc = _get_first_linked_address(customer.name, "Billing")
            except Exception as e:
                frappe.log_error(
                    f"Could not fetch billing address for {customer.name}: {str(e)}",
                    "Tally Ledger Address Fetch",
                )

        # 3) First Shipping
        if not address_doc:
            try:
                address_doc = _get_first_linked_address(customer.name, "Shipping")
            except Exception as e:
                frappe.log_error(
                    f"Could not fetch shipping address for {customer.name}: {str(e)}",
                    "Tally Ledger Address Fetch",
                )

        # 4) Fallback from GSTIN (if you implemented GST lookup)
        # 4) Fallback from GSTIN (optional, skip if not configured)
        if not address_doc and customer.gstin:
            try:
                from tally_connect.tally_integration.utils import get_address_from_gstin
                data = get_address_from_gstin(customer.gstin)
                if data:
                    class Dummy:
                        pass
                    d = Dummy()
                    d.address_line1 = data.get("address_line1")
                    d.address_line2 = data.get("address_line2")
                    d.city = data.get("city")
                    d.state = data.get("state")
                    d.pincode = data.get("pincode")
                    d.country = data.get("country") or "India"
                    address_doc = d
            except Exception:
                # GST lookup not configured or failed; continue without address
                pass


        # Build mailing details XML (Tally export style)
        addr_lines = []
        state = ""
        pincode = ""
        country = "India"

        if address_doc:
            if getattr(address_doc, "address_line1", None):
                addr_lines.append(address_doc.address_line1)
            if getattr(address_doc, "address_line2", None):
                addr_lines.append(address_doc.address_line2)
            if getattr(address_doc, "city", None):
                addr_lines.append(address_doc.city)
            state = getattr(address_doc, "state", "") or ""
            if getattr(address_doc, "pincode", None):
                pincode = str(address_doc.pincode)
            country = getattr(address_doc, "country", "") or "India"

        address_items = ""
        for line in addr_lines:
            address_items += f"\n           <ADDRESS>{escape_xml(line)}</ADDRESS>"

        mailing_details_xml = ""
        if addr_lines or state or pincode:
            mailing_details_xml = f"""
          <LEDMAILINGDETAILS.LIST>
           <ADDRESS.LIST TYPE="String">{address_items}
           </ADDRESS.LIST>
           <APPLICABLEFROM>20220401</APPLICABLEFROM>
           <PINCODE>{escape_xml(pincode)}</PINCODE>
           <MAILINGNAME>{escape_xml(customer.customer_name)}</MAILINGNAME>
           <STATE>{escape_xml(state)}</STATE>
           <COUNTRY>{escape_xml(country)}</COUNTRY>
          </LEDMAILINGDETAILS.LIST>"""

        # ---------------- GST DETAILS ----------------
        gst_reg_details_xml = ""
        if customer.gstin:
            gst_reg_details_xml = f"""
          <LEDGSTREGDETAILS.LIST>
           <APPLICABLEFROM>20220401</APPLICABLEFROM>
           <GSTREGISTRATIONTYPE>Regular</GSTREGISTRATIONTYPE>
           <PLACEOFSUPPLY>{escape_xml(state or "")}</PLACEOFSUPPLY>
           <GSTIN>{escape_xml(customer.gstin)}</GSTIN>
           <ISOTHTERRITORYASSESSEE>No</ISOTHTERRITORYASSESSEE>
           <CONSIDERPURCHASEFOREXPORT>No</CONSIDERPURCHASEFOREXPORT>
           <ISTRANSPORTER>No</ISTRANSPORTER>
           <ISCOMMONPARTY>No</ISCOMMONPARTY>
          </LEDGSTREGDETAILS.LIST>"""

        # ---------------- CONTACT DETAILS ----------------
        contact_person = customer.customer_name
        mobile = customer.mobile_no or ""
        contact_details_xml = ""
        if contact_person or mobile:
            contact_details_xml = f"""
          <CONTACTDETAILS.LIST>
           <NAME>{escape_xml(contact_person)}</NAME>
           <COUNTRYISDCODE>+91</COUNTRYISDCODE>
           <ISDEFAULTWHATSAPPNUM>Yes</ISDEFAULTWHATSAPPNUM>
          </CONTACTDETAILS.LIST>"""

        # ---------------- FINAL LEDGER XML ----------------
        ledger_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Masters</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE xmlns:UDF="TallyUDF">
        <LEDGER NAME="{escape_xml(customer.customer_name)}" RESERVEDNAME="">
          <PARENT>{escape_xml(parent_group)}</PARENT>
          <PRIORSTATENAME>{escape_xml(state or "")}</PRIORSTATENAME>
          <COUNTRYOFRESIDENCE>{escape_xml(country)}</COUNTRYOFRESIDENCE>
          <LEDGERCONTACT>{escape_xml(contact_person)}</LEDGERCONTACT>
          <LEDGERMOBILE>{escape_xml(mobile)}</LEDGERMOBILE>
          <LEDGERCOUNTRYISDCODE>+91</LEDGERCOUNTRYISDCODE>
          <PARTYGSTIN>{escape_xml(customer.gstin or "")}</PARTYGSTIN>
          <ISBILLWISEON>Yes</ISBILLWISEON>
          <ISCOSTCENTRESON>No</ISCOSTCENTRESON>
          <ISINTERESTON>No</ISINTERESTON>
          <LANGUAGENAME.LIST>
            <NAME.LIST TYPE="String">
              <NAME>{escape_xml(customer.customer_name)}</NAME>
            </NAME.LIST>
            <LANGUAGEID>1033</LANGUAGEID>
          </LANGUAGENAME.LIST>{gst_reg_details_xml}{mailing_details_xml}{contact_details_xml}
        </LEDGER>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""

        # Create sync log
        log = create_sync_log(
            operation_type="Create Customer Ledger",
            doctype_name="Customer",
            doc_name=customer_name,
            company=company,
            xml=ledger_xml,
        )

        # Send to Tally
        result = send_xml_to_tally(log, ledger_xml)

        if not result.get("success"):
            if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Customer",
                    document_name=customer_name,
                    operation="Create Ledger",
                    error_message=result.get("error", "Unknown error"),
                )
                return {
                    "success": False,
                    "error": result.get("error"),
                    "sync_log": log.name,
                    "retry_job": retry_job.name if retry_job else None,
                }

            return {
                "success": False,
                "error": result.get("error"),
                "sync_log": log.name,
            }

        # Mark customer as synced
        try:
            customer.db_set("custom_tally_synced", 1, update_modified=False)
            customer.db_set("custom_tally_sync_date", now(), update_modified=False)
        except Exception:
            pass

        return {
            "success": True,
            "message": f"Customer ledger '{customer.customer_name}' created in Tally",
            "sync_log": log.name,
        }

    except Exception as e:
        error_msg = f"Exception creating customer ledger: {str(e)}"
        frappe.log_error(error_msg, "Tally Ledger Creator")

        retry_job = create_retry_job(
            document_type="Customer",
            document_name=customer_name,
            operation="Create Ledger",
            error_message=error_msg,
        )

        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None,
        }


@frappe.whitelist()
def create_supplier_ledger_in_tally(supplier_name, company=None):
    """
    Create supplier ledger in Tally from ERPNext Supplier
    Similar to customer but uses supplier-specific parent group
    
    Args:
        supplier_name: ERPNext Supplier name
        company: ERPNext company name
    
    Returns:
        dict: {"success": bool, "message": str}
    """
    
    try:
        supplier = frappe.get_doc("Supplier", supplier_name)
        
        if not company:
            if supplier.accounts and len(supplier.accounts) > 0:
                company = supplier.accounts[0].company
            else:
                company = frappe.defaults.get_global_default("company")
        
        tally_company = get_tally_company_for_erpnext_company(company)
        parent_group = get_supplier_parent_group(supplier_name, company)
        
        # Check parent exists
        parent_check = check_master_exists("Group", parent_group)
        if not parent_check.get("exists"):
            error_msg = f"Parent group '{parent_group}' does not exist in Tally"
            retry_job = create_retry_job(
                document_type="Supplier",
                document_name=supplier_name,
                operation="Create Ledger",
                error_message=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # Check if exists
        exists_check = check_master_exists("Ledger", supplier.supplier_name)
        if exists_check.get("exists"):
            return {
                "success": False,
                "error": f"Ledger '{supplier.supplier_name}' already exists in Tally",
                "already_exists": True,
                "action_required": "UPDATE"
            }
        
        # Build address
        address_xml = ""
        if supplier.supplier_primary_address:
            try:
                address_doc = frappe.get_doc("Address", supplier.supplier_primary_address)
                address_lines = []
                if address_doc.address_line1:
                    address_lines.append(address_doc.address_line1)
                if address_doc.address_line2:
                    address_lines.append(address_doc.address_line2)
                if address_doc.city:
                    address_lines.append(address_doc.city)
                if address_doc.state:
                    address_lines.append(address_doc.state)
                if address_doc.pincode:
                    address_lines.append(address_doc.pincode)
                
                if address_lines:
                    address_xml = f"""
          <ADDRESS.LIST>
            <ADDRESS>{escape_xml(", ".join(address_lines))}</ADDRESS>
          </ADDRESS.LIST>"""
            except:
                pass
        
        # Build GSTIN
        gstin_xml = ""
        if hasattr(supplier, 'gstin') and supplier.gstin:
            gstin_xml = f"""
          <PARTYGSTIN.LIST>
            <PARTYGSTIN>{supplier.gstin}</PARTYGSTIN>
          </PARTYGSTIN.LIST>"""
        
        # Build XML
        ledger_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Masters</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <LEDGER NAME="{escape_xml(supplier.supplier_name)}" ACTION="Create">
          <NAME>{escape_xml(supplier.supplier_name)}</NAME>
          <PARENT>{escape_xml(parent_group)}</PARENT>
          <ISBILLWISEON>Yes</ISBILLWISEON>
          <AFFECTSSTOCK>No</AFFECTSSTOCK>{address_xml}{gstin_xml}
        </LEDGER>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""
        
        # Create log and send
        log = create_sync_log(
            operation_type="Create Supplier Ledger",
            doctype_name="Supplier",
            doc_name=supplier_name,
            company=company,
            xml=ledger_xml
        )
        
        result = send_xml_to_tally(log, ledger_xml)
        
        if not result.get("success"):
            if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Supplier",
                    document_name=supplier_name,
                    operation="Create Ledger",
                    error_message=result.get("error")
                )
                return {
                    "success": False,
                    "error": result.get("error"),
                    "sync_log": log.name,
                    "retry_job": retry_job.name if retry_job else None
                }
            
            return {
                "success": False,
                "error": result.get("error"),
                "sync_log": log.name
            }
        
        # Update supplier
        try:
            supplier.db_set("custom_tally_synced", 1, update_modified=False)
            supplier.db_set("custom_tally_sync_date", now(), update_modified=False)
        except:
            pass
        
        return {
            "success": True,
            "message": f"Supplier ledger '{supplier.supplier_name}' created in Tally",
            "sync_log": log.name
        }
    
    except Exception as e:
        error_msg = f"Exception creating supplier ledger: {str(e)}"
        frappe.log_error(error_msg, "Tally Ledger Creator")
        
        retry_job = create_retry_job(
            document_type="Supplier",
            document_name=supplier_name,
            operation="Create Ledger",
            error_message=error_msg
        )
        
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None
        }


# ============================================================================
# STOCK GROUP CREATOR
# ============================================================================

@frappe.whitelist()
def create_stock_group_in_tally(stock_group_name, parent_group="Primary", company=None):
    """
    Create stock group in Tally
    
    Args:
        stock_group_name: Name of stock group
        parent_group: Parent stock group (default: "Primary")
        company: ERPNext company name
    
    Returns:
        dict: {"success": bool, "message": str}
    """
    
    tally_company = get_tally_company_for_erpnext_company(company)
    
    # Check parent exists
    parent_check = check_master_exists("StockGroup", parent_group)
    if not parent_check.get("exists"):
        error_msg = f"Parent stock group '{parent_group}' does not exist in Tally"
        retry_job = create_retry_job(
            document_type="Stock Group",
            document_name=stock_group_name,
            operation="Create Stock Group",
            error_message=error_msg
        )
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None
        }
    
    # Check if exists
    exists_check = check_master_exists("StockGroup", stock_group_name)
    if exists_check.get("exists"):
        return {
            "success": False,
            "error": f"Stock Group '{stock_group_name}' already exists in Tally",
            "already_exists": True,
            "action_required": "UPDATE"
        }
    
    # Build XML
    stock_group_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Masters</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <STOCKGROUP NAME="{escape_xml(stock_group_name)}" ACTION="Create">
          <NAME>{escape_xml(stock_group_name)}</NAME>
          <PARENT>{escape_xml(parent_group)}</PARENT>
        </STOCKGROUP>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""
    
    log = create_sync_log(
        operation_type="Create Stock Group",
        doctype_name="Stock Group",
        doc_name=stock_group_name,
        company=company or "",
        xml=stock_group_xml
    )
    
    result = send_xml_to_tally(log, stock_group_xml)
    
    if not result.get("success"):
        if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
            retry_job = create_retry_job(
                document_type="Stock Group",
                document_name=stock_group_name,
                operation="Create Stock Group",
                error_message=result.get("error")
            )
            return {
                "success": False,
                "error": result.get("error"),
                "sync_log": log.name,
                "retry_job": retry_job.name if retry_job else None
            }
        
        return {
            "success": False,
            "error": result.get("error"),
            "sync_log": log.name
        }
    
    return {
        "success": True,
        "message": f"Stock Group '{stock_group_name}' created in Tally",
        "sync_log": log.name
    }


# ===========================================================================
# STOCK ITEM CREATOR (WITH ALTERNATE UNITS)
# ============================================================================

# @frappe.whitelist()
# def create_stock_item_in_tally(item_code, company=None):
#     """
#     Create stock item in Tally from ERPNext Item
    
#     KEY MAPPINGS:
#     - Item Name → Tally Stock Item NAME
#     - Item Code → Tally ALIAS
#     - Item UOM → Base Units
#     - Alternate UOMs → Alternate Units with conversion
    
#     Args:
#         item_code: ERPNext Item code
#         company: ERPNext company name
    
#     Returns:
#         dict: {"success": bool, "message": str}
    
#     Example:
#         create_stock_item_in_tally("ITEM-001", "Your Company")
#     """
    
#     try:
#         # Get item document
#         item = frappe.get_doc("Item", item_code)
        
#         tally_company = get_tally_company_for_erpnext_company(company)
        
#         # Get stock group
#         settings = get_settings()
#         stock_group = item.item_group or settings.default_inventory_stock_group or "Primary"
        
#         # Check stock group exists
#         group_check = check_master_exists("StockGroup", stock_group)
#         if not group_check.get("exists"):
#             error_msg = f"Stock Group '{stock_group}' does not exist in Tally"
#             retry_job = create_retry_job(
#                 document_type="Item",
#                 document_name=item_code,
#                 operation="Create Stock Item",
#                 error_message=error_msg
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None
#             }
        
#         # Check base unit exists
#         unit_check = check_master_exists("Unit", item.stock_uom)
#         if not unit_check.get("exists"):
#             error_msg = f"Unit '{item.stock_uom}' does not exist in Tally"
#             retry_job = create_retry_job(
#                 document_type="Item",
#                 document_name=item_code,
#                 operation="Create Stock Item",
#                 error_message=error_msg
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None
#             }
        
#         # Check if item exists (using Item Name as per requirement)
#         exists_check = check_master_exists("StockItem", item.item_name)
#         if exists_check.get("exists"):
#             return {
#                 "success": False,
#                 "error": f"Stock Item '{item.item_name}' already exists in Tally",
#                 "already_exists": True,
#                 "action_required": "UPDATE"
#             }
        
#         # Build GST Classification XML
#         gst_xml = ""
#         if hasattr(item, 'custom_gst_hsn_code') and item.custom_gst_hsn_code:
#             # Check if GST classification exists
#             gst_check = check_master_exists("GSTClassification", item.custom_gst_hsn_code)
#             if gst_check.get("exists"):
#                 gst_xml = f"""
#           <GSTAPPLICABLE>Applicable</GSTAPPLICABLE>
#           <GSTCLASSIFICATIONNAME>{escape_xml(item.custom_gst_hsn_code)}</GSTCLASSIFICATIONNAME>"""
        
#         # Build HSN Code XML
#         hsn_xml = ""
#         if item.gst_hsn_code:
#             hsn_xml = f"""
#           <HSNCODE>{item.gst_hsn_code}</HSNCODE>"""
        
#         # Build Alternate Units XML (CRITICAL: Box/Pcs conversion)
#         alternate_units_xml = ""
#         if item.uoms and len(item.uoms) > 0:
#             for uom_row in item.uoms:
#                 # Check if alternate unit exists in Tally
#                 alt_unit_check = check_master_exists("Unit", uom_row.uom)
#                 if alt_unit_check.get("exists"):
#                     conversion = flt(uom_row.conversion_factor) or 1
#                     alternate_units_xml += f"""
#           <MULTIPLEUNITS.LIST>
#             <REPORTINGUOM>{escape_xml(uom_row.uom)}</REPORTINGUOM>
#             <CONVERSIONFACTOR>{conversion}</CONVERSIONFACTOR>
#             <BASEUNITS>{escape_xml(item.stock_uom)}</BASEUNITS>
#           </MULTIPLEUNITS.LIST>"""
        
#         # Build Stock Item XML
#         stock_item_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#   <HEADER>
#     <VERSION>1</VERSION>
#     <TALLYREQUEST>Import</TALLYREQUEST>
#     <TYPE>Data</TYPE>
#     <ID>All Masters</ID>
#   </HEADER>
#   <BODY>
#     <DESC>
#       <STATICVARIABLES>
#         <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
#       </STATICVARIABLES>
#     </DESC>
#     <DATA>
#       <TALLYMESSAGE>
#         <STOCKITEM NAME="{escape_xml(item.item_name)}" ACTION="Create">
#           <NAME>{escape_xml(item.item_name)}</NAME>
#           <ALIAS>{escape_xml(item.item_code)}</ALIAS>
#           <PARENT>{escape_xml(stock_group)}</PARENT>
#           <BASEUNITS>{escape_xml(item.stock_uom)}</BASEUNITS>{gst_xml}{hsn_xml}{alternate_units_xml}
#         </STOCKITEM>
#       </TALLYMESSAGE>
#     </DATA>
#   </BODY>
# </ENVELOPE>"""
        
#         # Create log and send
#         log = create_sync_log(
#             operation_type="Create Stock Item",
#             doctype_name="Item",
#             doc_name=item_code,
#             company=company or "",
#             xml=stock_item_xml
#         )
        
#         result = send_xml_to_tally(log, stock_item_xml)
        
#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Item",
#                     document_name=item_code,
#                     operation="Create Stock Item",
#                     error_message=result.get("error")
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None
#                 }
            
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name
#             }
        
#         # Update item
#         try:
#             item.db_set("custom_tally_synced", 1, update_modified=False)
#             item.db_set("custom_tally_sync_date", now(), update_modified=False)
#         except:
#             pass
        
#         return {
#             "success": True,
#             "message": f"Stock Item '{item.item_name}' (Code: {item.item_code}) created in Tally",
#             "sync_log": log.name
#         }
    
#     except Exception as e:
#         error_msg = f"Exception creating stock item: {str(e)}"
#         frappe.log_error(error_msg, "Tally Stock Item Creator")
        
#         retry_job = create_retry_job(
#             document_type="Item",
#             document_name=item_code,
#             operation="Create Stock Item",
#             error_message=error_msg
#         )
        
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None
#         }
@frappe.whitelist()
def create_stock_item_in_tally(item_code, company=None):
    """
    Create stock item in Tally from ERPNext Item

    KEY MAPPINGS:
    - Item Name → Tally Stock Item NAME
    - Item Code → Alias (second NAME in LANGUAGENAME.LIST)
    - Item UOM → BASEUNITS
    - Alternate UOM → ADDITIONALUNITS + DENOMINATOR + CONVERSION
    - GST / HSN → GST Classification (if available) or Company/Stock Group
    """

    try:
        # 0. Get item document
        item = frappe.get_doc("Item", item_code)

        # Determine Tally company (reserved for future use)
        tally_company = get_tally_company_for_erpnext_company(company)

        # Get stock group from Item → Settings → Primary
        settings = get_settings()
        stock_group = item.item_group or settings.default_inventory_stock_group or "Primary"

        # ---------- 1. Ensure Stock Group exists (with GST fallback) ----------

        group_check = check_master_exists("StockGroup", stock_group)
        if not group_check.get("exists"):
            # Check if GST Classification with same name exists
            gst_class_check = check_master_exists("GSTClassification", stock_group)

            if gst_class_check.get("exists"):
                stock_group_gst_xml = f"""
        <GSTDETAILS.LIST>
          <APPLICABLEFROM>20250401</APPLICABLEFROM>
          <HSNMASTERNAME>{escape_xml(stock_group)}</HSNMASTERNAME>
          <SRCOFGSTDETAILS>Use GST Classification</SRCOFGSTDETAILS>
        </GSTDETAILS.LIST>"""
            else:
                stock_group_gst_xml = """
        <GSTDETAILS.LIST>
          <APPLICABLEFROM>20250401</APPLICABLEFROM>
          <SRCOFGSTDETAILS>Specify Details Here</SRCOFGSTDETAILS>
        </GSTDETAILS.LIST>"""

            stock_group_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Masters</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <STOCKGROUP NAME="{escape_xml(stock_group)}" ACTION="Create">
          <NAME>{escape_xml(stock_group)}</NAME>
          <PARENT>Primary</PARENT>
          {stock_group_gst_xml}
        </STOCKGROUP>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""

            group_log = create_sync_log(
                operation_type="Create Stock Group",
                doctype_name="Stock Group",
                doc_name=stock_group,
                company=company or "",
                xml=stock_group_xml,
            )
            group_result = send_xml_to_tally(group_log, stock_group_xml)

            if not group_result.get("success"):
                retry_job = None
                if group_result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                    retry_job = create_retry_job(
                        document_type="Item",
                        document_name=item_code,
                        operation="Create Stock Group",
                        error_message=group_result.get("error"),
                    )
                return {
                    "success": False,
                    "error": group_result.get("error"),
                    "sync_log": group_log.name,
                    "retry_job": retry_job.name if retry_job else None,
                }

        # ---------- 2. Check base unit exists ----------

        unit_check = check_master_exists("Unit", item.stock_uom)
        if not unit_check.get("exists"):
            error_msg = f"Unit '{item.stock_uom}' does not exist in Tally"
            retry_job = create_retry_job(
                document_type="Item",
                document_name=item_code,
                operation="Create Stock Item",
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
            }

        # ---------- 3. Check if Stock Item already exists ----------

        exists_check = check_master_exists("StockItem", item.item_name)
        if exists_check.get("exists"):
            return {
                "success": False,
                "error": f"Stock Item '{item.item_name}' already exists in Tally",
                "already_exists": True,
                "action_required": "UPDATE",
            }

        # ---------- 4. Build GST / HSN XML for ITEM with fallback ----------

        gst_details_xml = ""
        hsn_details_xml = ""

        gst_class_item_check = check_master_exists("GSTClassification", stock_group)

        if gst_class_item_check.get("exists"):
            gst_details_xml = f"""
          <GSTDETAILS.LIST>
            <APPLICABLEFROM>20250401</APPLICABLEFROM>
            <HSNMASTERNAME>{escape_xml(stock_group)}</HSNMASTERNAME>
            <SRCOFGSTDETAILS>Use GST Classification</SRCOFGSTDETAILS>
          </GSTDETAILS.LIST>"""

            hsn_details_xml = f"""
          <HSNDETAILS.LIST>
            <APPLICABLEFROM>20250401</APPLICABLEFROM>
            <HSNCLASSIFICATIONNAME>{escape_xml(stock_group)}</HSNCLASSIFICATIONNAME>
            <SRCOFHSNDETAILS>Use GST Classification</SRCOFHSNDETAILS>
          </HSNDETAILS.LIST>"""
        else:
            gst_details_xml = """
          <GSTDETAILS.LIST>
            <APPLICABLEFROM>20250401</APPLICABLEFROM>
            <SRCOFGSTDETAILS>As per Company/Stock Group</SRCOFGSTDETAILS>
          </GSTDETAILS.LIST>"""

            hsn_details_xml = """
          <HSNDETAILS.LIST>
            <APPLICABLEFROM>20250401</APPLICABLEFROM>
            <SRCOFHSNDETAILS>As per Company/Stock Group</SRCOFHSNDETAILS>
          </HSNDETAILS.LIST>"""

        # ---------- 5. Alternate Units (BASEUNITS / ADDITIONALUNITS / DENOMINATOR / CONVERSION) ----------

        box_uom = None
        box_conv = None

        if getattr(item, "uoms", None):
            for uom_row in item.uoms:
                if not uom_row.uom or uom_row.uom == item.stock_uom:
                    continue

                alt_unit_check = check_master_exists("Unit", uom_row.uom)
                if not alt_unit_check.get("exists"):
                    continue

                box_uom = uom_row.uom
                box_conv = flt(uom_row.conversion_factor) or 1
                break

        extra_uom_xml = ""
        if box_uom:
            # Match your exported Tally XML exactly
            extra_uom_xml = f"""
          <ADDITIONALUNITS>{escape_xml(box_uom)}</ADDITIONALUNITS>
          <DENOMINATOR> {box_conv}</DENOMINATOR>
          <CONVERSION> 1</CONVERSION>"""

        # ---------- 6. Build Stock Item XML ----------

        stock_item_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Masters</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>@@DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <STOCKITEM NAME="{escape_xml(item.item_name)}" ACTION="Create">
          <PARENT>{escape_xml(stock_group)}</PARENT>
          <GSTAPPLICABLE>Applicable</GSTAPPLICABLE>
          <GSTTYPEOFSUPPLY>Goods</GSTTYPEOFSUPPLY>
          <COSTINGMETHOD>Avg. Cost</COSTINGMETHOD>
          <VALUATIONMETHOD>Avg. Price</VALUATIONMETHOD>
          <BASEUNITS>{escape_xml(item.stock_uom)}</BASEUNITS>
          {extra_uom_xml}
          {gst_details_xml}
          {hsn_details_xml}
          <LANGUAGENAME.LIST>
            <NAME.LIST TYPE="String">
              <NAME>{escape_xml(item.item_name)}</NAME>
              <NAME>{escape_xml(item.item_code)}</NAME>
            </NAME.LIST>
            <LANGUAGEID>1033</LANGUAGEID>
          </LANGUAGENAME.LIST>
        </STOCKITEM>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""

        # ---------- 7. Log and send to Tally ----------

        log = create_sync_log(
            operation_type="Create Stock Item",
            doctype_name="Item",
            doc_name=item_code,
            company=company or "",
            xml=stock_item_xml,
        )

        result = send_xml_to_tally(log, stock_item_xml)

        if not result.get("success"):
            if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Item",
                    document_name=item_code,
                    operation="Create Stock Item",
                    error_message=result.get("error"),
                )
                return {
                    "success": False,
                    "error": result.get("error"),
                    "sync_log": log.name,
                    "retry_job": retry_job.name if retry_job else None,
                }

            return {
                "success": False,
                "error": result.get("error"),
                "sync_log": log.name,
            }

        # ---------- 8. Mark item as synced ----------

        try:
            item.db_set("custom_tally_synced", 1, update_modified=False)
            item.db_set("custom_tally_sync_date", now(), update_modified=False)
        except Exception:
            pass

        return {
            "success": True,
            "message": f"Stock Item '{item.item_name}' (Code: {item.item_code}) created in Tally",
            "sync_log": log.name,
        }

    except Exception as e:
        error_msg = f"Exception creating stock item: {str(e)}"
        frappe.log_error(error_msg, "Tally Stock Item Creator")

        retry_job = create_retry_job(
            document_type="Item",
            document_name=item_code,
            operation="Create Stock Item",
            error_message=error_msg,
        )

        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None,
        }

@frappe.whitelist()
def create_sales_invoice_in_tally(invoice_name):
    """
    Create Sales Invoice voucher in Tally from ERPNext Sales Invoice
    
    Pre-sync validations:
    - Customer ledger exists (auto-create if missing)
    - Sales ledger exists
    - GST ledgers exist (CGST/SGST/IGST)
    - Round Off ledger exists
    - Stock items exist
    
    Args:
        invoice_name: ERPNext Sales Invoice name
    
    Returns:
        dict: {"success": bool, "message": str, "voucher_number": str}
    """
    
    try:
        # ---------- 1. Load Sales Invoice ----------
        
        inv = frappe.get_doc("Sales Invoice", invoice_name)
        
        if inv.docstatus != 1:
            return {
                "success": False,
                "error": "Sales Invoice must be submitted before syncing to Tally"
            }
        
        # Get settings
        settings = get_settings()
        
        # Get Tally company name
        tally_company = get_tally_company_for_erpnext_company(inv.company)
        
        if not tally_company:
            return {
                "success": False,
                "error": "No Tally company mapped for this ERPNext company"
            }
        
        # ---------- 2. Validate/Create Customer Ledger ----------
        
        customer_name = inv.customer_name
        
        customer_check = check_master_exists("Ledger", customer_name)
        if not customer_check.get("exists"):
            # Auto-create customer ledger
            customer_result = create_customer_ledger_in_tally(inv.customer, inv.company)
            
            if not customer_result.get("success"):
                return {
                    "success": False,
                    "error": f"Customer ledger '{customer_name}' does not exist and auto-creation failed: {customer_result.get('error')}",
                    "retry_job": customer_result.get("retry_job")
                }
        
        # ---------- 3. Validate Required Ledgers ----------
        
        required_ledgers = {}
        
        # Sales ledger
        sales_ledger = settings.sales_ledger_name or "SALES A/C"
        required_ledgers["Sales"] = sales_ledger
        
        # GST ledgers
        cgst_ledger = settings.cgst_ledger_name or "CGST"
        sgst_ledger = settings.sgst_ledger_name or "SGST"
        igst_ledger = settings.igst_ledger_name or "IGST"
        
        required_ledgers["CGST"] = cgst_ledger
        required_ledgers["SGST"] = sgst_ledger
        required_ledgers["IGST"] = igst_ledger
        
        # Round off ledger
        roundoff_ledger = settings.roundoff_ledger_name or "Round Off"
        required_ledgers["Round Off"] = roundoff_ledger
        
        # Check all required ledgers
        missing_ledgers = []
        for ledger_type, ledger_name in required_ledgers.items():
            ledger_check = check_master_exists("Ledger", ledger_name)
            if not ledger_check.get("exists"):
                missing_ledgers.append(f"{ledger_type} ({ledger_name})")
        
        if missing_ledgers:
            error_msg = f"Missing ledgers in Tally: {', '.join(missing_ledgers)}"
            retry_job = create_retry_job(
                document_type="Sales Invoice",
                document_name=invoice_name,
                operation="Create Sales Invoice",
                error_message=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # ---------- 4. Validate Stock Items ----------
        
        missing_items = []
        for item in inv.items:
            item_check = check_master_exists("StockItem", item.item_name)
            if not item_check.get("exists"):
                missing_items.append(item.item_name)
        
        if missing_items:
            error_msg = f"Missing stock items in Tally: {', '.join(missing_items[:5])}"
            if len(missing_items) > 5:
                error_msg += f" and {len(missing_items) - 5} more"
            
            retry_job = create_retry_job(
                document_type="Sales Invoice",
                document_name=invoice_name,
                operation="Create Sales Invoice",
                error_message=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # ---------- 5. Build Voucher XML ----------
        
        # Helper function for quantity display
        def qty_display(qty, uom, per_box=6):
            base = int(abs(qty))
            boxes = int(abs(qty) // per_box)
            if uom:
                return f" {base} {uom} =  {boxes} Box"
            return f" {base} = {boxes} Box"
        
        # Date conversions
        invoice_date = format_date_for_tally(inv.posting_date)
        po_date_str = format_date_for_tally(inv.po_date) if inv.po_date else ""
        lr_date_str = format_date_for_tally(inv.lr_date) if inv.lr_date else ""
        
        # Other fields
        po_no = escape_xml(inv.po_no or "")
        expiry_date_str = inv.custom_expiry_date or "" if hasattr(inv, "custom_expiry_date") else ""
        expiry_ref = f"Expiry Date: {expiry_date_str}" if expiry_date_str else ""
        
        # Place of supply
        place_of_supply = inv.place_of_supply or "India"
        if "-" in place_of_supply:
            state_name = place_of_supply.split("-")[1].strip()
        else:
            state_name = place_of_supply
        
        destination = state_name
        transporter_name = escape_xml(inv.transporter_name or "")
        payment_terms = "30 Days"
        
        # Calculate totals
        total_taxable = float(inv.total or 0)
        total_igst = 0.0
        total_cgst = 0.0
        total_sgst = 0.0
        
        for tax_line in inv.taxes:
            gst_type = (tax_line.gst_tax_type or "").lower()
            tax_amount = float(tax_line.tax_amount or 0)
            
            if gst_type == "igst":
                total_igst += tax_amount
            elif gst_type == "cgst":
                total_cgst += tax_amount
            elif gst_type == "sgst":
                total_sgst += tax_amount
        
        total_igst = round(total_igst, 2)
        total_cgst = round(total_cgst, 2)
        total_sgst = round(total_sgst, 2)
        
        grand_total = round(float(inv.base_rounded_total or inv.grand_total or 0), 2)
        roundoff = float(inv.rounding_adjustment or 0)
        
        # Determine interstate
        interstate = False
        company_gstin = inv.company_gstin or ""
        customer_gstin = inv.billing_address_gstin or inv.customer_gstin or ""
        
        if customer_gstin and company_gstin:
            interstate = (customer_gstin[:2] != company_gstin[:2])
        
        # Build address XML
        addr_lines = ""
        try:
            if inv.customer_address:
                billing_addr = frappe.get_doc("Address", inv.customer_address).as_dict()
                
                if billing_addr.get("address_line1"):
                    addr_lines += f"\n       <ADDRESS>{escape_xml(billing_addr['address_line1'])}</ADDRESS>"
                if billing_addr.get("address_line2"):
                    addr_lines += f"\n       <ADDRESS>{escape_xml(billing_addr['address_line2'])}</ADDRESS>"
                
                city_line = ""
                if billing_addr.get("city"):
                    city_line = billing_addr["city"]
                if billing_addr.get("state"):
                    city_line += f", {billing_addr['state']}" if city_line else billing_addr["state"]
                if billing_addr.get("pincode"):
                    city_line += f" - {billing_addr['pincode']}"
                
                if city_line:
                    addr_lines += f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"
        except Exception:
            pass
        
        # Build inventory items XML
        items_xml = ""
        
        for item in inv.items:
            stock_group = item.item_group or "Primary"
            
            # Quantity calculation
            if item.qty > 6:
                qty = int((item.qty) // 6) * 6
            else:
                qty = int(item.qty)
            
            qty_str = qty_display(qty, item.uom, per_box=6)
            
            # GST rates
            cgst_rate = int(round(item.cgst_rate or 0)) if hasattr(item, "cgst_rate") else 0
            sgst_rate = int(round(item.sgst_rate or 0)) if hasattr(item, "sgst_rate") else 0
            igst_rate = int(round(item.igst_rate or 0)) if hasattr(item, "igst_rate") else 0
            
            # MRP
            item_mrp_text = ""
            try:
                mrp_value = int(frappe.db.get_value("Item", {"item_name": item.item_name}, "custom_mrp") or 0)
                if mrp_value:
                    item_mrp_text = f"MRP {mrp_value}"
            except Exception:
                pass
            
            mrp_xml = f"""
      <BASICUSERDESCRIPTION.LIST TYPE="String">
        <BASICUSERDESCRIPTION>{item_mrp_text}</BASICUSERDESCRIPTION>
       </BASICUSERDESCRIPTION.LIST>""" if item_mrp_text else ""
            
            items_xml += f"""
      <ALLINVENTORYENTRIES.LIST>{mrp_xml}
       <STOCKITEMNAME>{escape_xml(item.item_name)}</STOCKITEMNAME>
       <GSTOVRDNCLASSIFICATION>{escape_xml(stock_group)}</GSTOVRDNCLASSIFICATION>
       <GSTOVRDNINELIGIBLEITC> Not Applicable</GSTOVRDNINELIGIBLEITC>
       <GSTOVRDNISREVCHARGEAPPL> Not Applicable</GSTOVRDNISREVCHARGEAPPL>
       <GSTOVRDNTAXABILITY>Taxable</GSTOVRDNTAXABILITY>
       <GSTSOURCETYPE>Stock Group</GSTSOURCETYPE>
       <HSNSOURCETYPE>Stock Group</HSNSOURCETYPE>
       <GSTOVRDNTYPEOFSUPPLY>Goods</GSTOVRDNTYPEOFSUPPLY>
       <GSTRATEINFERAPPLICABILITY>Use GST Classification</GSTRATEINFERAPPLICABILITY>
       <GSTHSNINFERAPPLICABILITY>Use GST Classification</GSTHSNINFERAPPLICABILITY>
       <HSNOVRDNCLASSIFICATION>{escape_xml(stock_group)}</HSNOVRDNCLASSIFICATION>
       <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
       <ISGSTASSESSABLEVALUEOVERRIDDEN>No</ISGSTASSESSABLEVALUEOVERRIDDEN>
       <RATE>{item.rate}/{item.uom}</RATE>
       <AMOUNT>{item.amount}</AMOUNT>
       <ACTUALQTY>{qty_str}</ACTUALQTY>
       <BILLEDQTY>{qty_str}</BILLEDQTY>
       <BATCHALLOCATIONS.LIST>
        <GODOWNNAME>Main Location</GODOWNNAME>
        <BATCHNAME>Primary Batch</BATCHNAME>
        <AMOUNT>{item.amount}</AMOUNT>
        <ACTUALQTY>{qty_str}</ACTUALQTY>
        <BILLEDQTY>{qty_str}</BILLEDQTY>
       </BATCHALLOCATIONS.LIST>
       <ACCOUNTINGALLOCATIONS.LIST>
        <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
        <AMOUNT>{item.amount}</AMOUNT>
       </ACCOUNTINGALLOCATIONS.LIST>
       <RATEDETAILS.LIST>
        <GSTRATEDUTYHEAD>CGST</GSTRATEDUTYHEAD>
        <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
        <GSTRATE> {cgst_rate:.2f}</GSTRATE>
       </RATEDETAILS.LIST>
       <RATEDETAILS.LIST>
        <GSTRATEDUTYHEAD>SGST/UTGST</GSTRATEDUTYHEAD>
        <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
        <GSTRATE> {sgst_rate:.2f}</GSTRATE>
       </RATEDETAILS.LIST>
       <RATEDETAILS.LIST>
        <GSTRATEDUTYHEAD>IGST</GSTRATEDUTYHEAD>
        <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
        <GSTRATE> {igst_rate:.2f}</GSTRATE>
       </RATEDETAILS.LIST>
       <RATEDETAILS.LIST>
        <GSTRATEDUTYHEAD>Cess</GSTRATEDUTYHEAD>
        <GSTRATEVALUATIONTYPE> Not Applicable</GSTRATEVALUATIONTYPE>
       </RATEDETAILS.LIST>
       <RATEDETAILS.LIST>
        <GSTRATEDUTYHEAD>State Cess</GSTRATEDUTYHEAD>
        <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
       </RATEDETAILS.LIST>
      </ALLINVENTORYENTRIES.LIST>"""
        
        # Party ledger amount (negative)
        party_amount = -1 * grand_total
        
        # Build main XML
        xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Import Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <IMPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Vouchers</REPORTNAME>
    <STATICVARIABLES>
     <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
    </STATICVARIABLES>
   </REQUESTDESC>
   <REQUESTDATA>
    <TALLYMESSAGE xmlns:UDF="TallyUDF">
     <VOUCHER REMOTEID="" VCHKEY="" VCHTYPE="Sales" ACTION="Create" OBJVIEW="Invoice Voucher View">
      <ADDRESS.LIST TYPE="String">
       <ADDRESS>{escape_xml(customer_name)}</ADDRESS>{addr_lines}
      </ADDRESS.LIST>
      <BASICBUYERADDRESS.LIST TYPE="String">
       <BASICBUYERADDRESS>{escape_xml(customer_name)}</BASICBUYERADDRESS>{addr_lines}
      </BASICBUYERADDRESS.LIST>
      <OLDAUDITENTRYIDS.LIST TYPE="Number">
       <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
      </OLDAUDITENTRYIDS.LIST>"""
        
        # Add order details if present
        if po_no or expiry_ref:
            xml_body += f"""
      <INVOICEORDERLIST.LIST>
       <BASICORDERDATE>{po_date_str}</BASICORDERDATE>
       <BASICPURCHASEORDERNO>{po_no}</BASICPURCHASEORDERNO>
       <BASICOTHERREFERENCES>{escape_xml(expiry_ref)}</BASICOTHERREFERENCES>
      </INVOICEORDERLIST.LIST>"""
        
        xml_body += f"""
      <BASICFINALDESTINATION>{escape_xml(destination)}</BASICFINALDESTINATION>
      <BASICORDERREF>{escape_xml(expiry_ref)}</BASICORDERREF>
      <BASICDUEDATEOFPYMT>{payment_terms}</BASICDUEDATEOFPYMT>
      <BASICSHIPPEDBY>{transporter_name}</BASICSHIPPEDBY>
      <DATE>{invoice_date}</DATE>
      <ISINVOICE>Yes</ISINVOICE>
      <STATENAME>{escape_xml(state_name)}</STATENAME>
      <COUNTRYOFRESIDENCE>India</COUNTRYOFRESIDENCE>
      <PARTYGSTIN>{escape_xml(customer_gstin)}</PARTYGSTIN>
      <PLACEOFSUPPLY>{escape_xml(state_name)}</PLACEOFSUPPLY>
      <PARTYNAME>{escape_xml(customer_name)}</PARTYNAME>
      <VOUCHERTYPENAME>Sales</VOUCHERTYPENAME>
      <VOUCHERNUMBER>{escape_xml(inv.name)}</VOUCHERNUMBER>
      <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
      <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
      <VCHGSTSTATUSISAPPLICABLE>Yes</VCHGSTSTATUSISAPPLICABLE>
      {items_xml}
      <LEDGERENTRIES.LIST>
       <OLDAUDITENTRYIDS.LIST TYPE="Number">
        <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
       </OLDAUDITENTRYIDS.LIST>
       <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
       <ISLASTDEEMEDPOSITIVE>Yes</ISLASTDEEMEDPOSITIVE>
       <AMOUNT>{party_amount:.2f}</AMOUNT>
      </LEDGERENTRIES.LIST>"""
        
        # Add tax ledger entries
        if not interstate:
            if total_cgst > 0:
                xml_body += f"""
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <AMOUNT>{total_cgst:.2f}</AMOUNT>
       <VATEXPAMOUNT>{total_cgst:.2f}</VATEXPAMOUNT>
      </LEDGERENTRIES.LIST>"""
            
            if total_sgst > 0:
                xml_body += f"""
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <AMOUNT>{total_sgst:.2f}</AMOUNT>
       <VATEXPAMOUNT>{total_sgst:.2f}</VATEXPAMOUNT>
      </LEDGERENTRIES.LIST>"""
        else:
            if total_igst > 0:
                xml_body += f"""
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <AMOUNT>{total_igst:.2f}</AMOUNT>
       <VATEXPAMOUNT>{total_igst:.2f}</VATEXPAMOUNT>
      </LEDGERENTRIES.LIST>"""
        
        # Add roundoff if needed
        if abs(roundoff) >= 0.01:
            roundoff_sign = "No" if roundoff > 0 else "Yes"
            xml_body += f"""
      <LEDGERENTRIES.LIST>
       <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
       <LEDGERNAME>{escape_xml(roundoff_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <ISLASTDEEMEDPOSITIVE>{roundoff_sign}</ISLASTDEEMEDPOSITIVE>
       <ROUNDLIMIT> 1</ROUNDLIMIT>
       <AMOUNT>{roundoff:.2f}</AMOUNT>
       <VATEXPAMOUNT>{roundoff:.2f}</VATEXPAMOUNT>
      </LEDGERENTRIES.LIST>"""
        
        # Close XML
        xml_body += """
     </VOUCHER>
    </TALLYMESSAGE>
   </REQUESTDATA>
  </IMPORTDATA>
 </BODY>
</ENVELOPE>"""
        
        # ---------- 6. Log and Send to Tally ----------
        
        log = create_sync_log(
            operation_type="Create Sales Invoice",
            doctype_name="Sales Invoice",
            doc_name=invoice_name,
            company=inv.company,
            xml=xml_body
        )
        
        result = send_xml_to_tally(log, xml_body)
        
        if not result.get("success"):
            if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Sales Invoice",
                    document_name=invoice_name,
                    operation="Create Sales Invoice",
                    error_message=result.get("error")
                )
                return {
                    "success": False,
                    "error": result.get("error"),
                    "sync_log": log.name,
                    "retry_job": retry_job.name if retry_job else None
                }
            
            return {
                "success": False,
                "error": result.get("error"),
                "sync_log": log.name
            }
        
        # ---------- 7. Extract voucher number and mark as synced ----------
        
        voucher_number = inv.name
        response_text = result.get("response", "")
        
        vch_start = response_text.find("<VOUCHERNUMBER>")
        if vch_start != -1:
            vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
            if vch_end != -1:
                voucher_number = response_text[vch_start + 15:vch_end].strip()
        
        # Update invoice
        try:
            inv.db_set("custom_posted_to_tally", 1, update_modified=False)
            inv.db_set("custom_tally_voucher_number", voucher_number, update_modified=False)
            inv.db_set("custom_tally_push_status", "Success", update_modified=False)
            inv.db_set("custom_tally_sync_date", now(), update_modified=False)
            frappe.db.commit()
        except Exception:
            pass
        
        return {
            "success": True,
            "message": f"Sales Invoice '{inv.name}' created in Tally",
            "voucher_number": voucher_number,
            "sync_log": log.name
        }
    
    except Exception as e:
        error_msg = f"Exception creating sales invoice: {str(e)}"
        frappe.log_error(error_msg, "Tally Sales Invoice Creator")
        
        retry_job = create_retry_job(
            document_type="Sales Invoice",
            document_name=invoice_name,
            operation="Create Sales Invoice",
            error_message=error_msg
        )
        
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None
        }
