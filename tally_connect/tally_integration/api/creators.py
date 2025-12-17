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
    get_address_from_gstin,
    get_tally_company_for_erpnext_company,
)
from tally_connect.tally_integration.api.checkers import check_ledger_exists


# 
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
# def create_generic_ledger_in_tally(ledger_name, parent_group, company=None):
#     """
#     Create a generic ledger (Sales, GST, Round Off, etc.) in Tally.

#     Args:
#         ledger_name (str): Ledger name in Tally (e.g., 'SALES A/C', 'CGST')
#         parent_group (str): Parent group in Tally (e.g., 'Sales Accounts')
#         company (str): ERPNext Company (optional)

#     Returns:
#         dict: {
#             "success": bool,
#             "message": str,
#             "log_id": str (optional),
#             "already_exists": bool (optional),
#             "error": str (on failure)
#         }
#     """
#     try:
#         settings = get_settings()
#         if not settings.enabled:
#             return {
#                 "success": False,
#                 "error": "Tally integration is disabled in settings",
#             }

#         if not ledger_name:
#             return {"success": False, "error": "ledger_name is required"}

#         if not company:
#             company = settings.erpnext_company or frappe.defaults.get_user_default("Company")

#         # Resolve Tally company
#         tally_company = get_tally_company_for_erpnext_company(company) or settings.tally_company_name
#         if not tally_company:
#             return {
#                 "success": False,
#                 "error": f"No Tally company mapped for ERPNext company '{company}'",
#             }

#         # Already exists?
#         existing = check_ledger_exists(ledger_name)
#         if existing.get("exists"):
#             return {
#                 "success": True,
#                 "already_exists": True,
#                 "message": f"Ledger '{ledger_name}' already exists in Tally",
#             }

#         if not parent_group:
#             return {
#                 "success": False,
#                 "error": f"Parent group not provided for ledger '{ledger_name}'",
#             }

#         # Build XML
#         xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#  <HEADER>
#   <TALLYREQUEST>Import Data</TALLYREQUEST>
#  </HEADER>
#  <BODY>
#   <IMPORTDATA>
#    <REQUESTDESC>
#     <REPORTNAME>All Masters</REPORTNAME>
#     <STATICVARIABLES>
#      <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
#     </STATICVARIABLES>
#    </REQUESTDESC>
#    <REQUESTDATA>
#     <TALLYMESSAGE xmlns:UDF="TallyUDF">
#      <LEDGER NAME="{escape_xml(ledger_name)}" ACTION="Create">
#       <NAME>{escape_xml(ledger_name)}</NAME>
#       <PARENT>{escape_xml(parent_group)}</PARENT>
#       <ISBILLWISEON>No</ISBILLWISEON>
#       <ISCOSTCENTRESON>No</ISCOSTCENTRESON>
#      </LEDGER>
#     </TALLYMESSAGE>
#    </REQUESTDATA>
#   </IMPORTDATA>
#  </BODY>
# </ENVELOPE>"""

#         # Log + send
#         log = create_sync_log(
#             operation_type="Create Generic Ledger",
#             doctype_name="Account",
#             doc_name=ledger_name,
#             company=company,
#             xml=xml_body,
#         )
#         result = send_xml_to_tally(log, xml_body)

#         if not result.get("success"):
#             return {
#                 "success": False,
#                 "error": result.get("error") or "Unknown error creating ledger",
#                 "error_type": result.get("error_type"),
#                 "log_id": log.name,
#             }

#         return {
#             "success": True,
#             "message": f"Ledger '{ledger_name}' created in Tally under '{parent_group}'",
#             "log_id": log.name,
#         }

#     except Exception as e:
#         msg = f"Exception creating generic ledger '{ledger_name}': {str(e)}"
#         frappe.log_error("Tally Generic Ledger Creator", msg[:1000])
#         return {"success": False, "error": str(e)}

@frappe.whitelist()
def create_generic_ledger_in_tally(ledger_name, parent_group, company=None):
    """
    Create a generic ledger (Sales, GST, Round Off, etc.) in Tally.

    Args:
        ledger_name (str): Ledger name in Tally (e.g. 'SALES A/C', 'CGST')
        parent_group (str): Parent group in Tally (e.g. 'Sales Accounts')
        company (str): ERPNext Company (optional)

    Returns:
        {
            "success": bool,
            "message": str,
            "log_id": str (optional),
            "already_exists": bool (optional),
            "error": str (on failure),
            "error_type": str (on failure)
        }
    """

    try:
        settings = get_settings()
        if not settings.enabled:
            return {
                "success": False,
                "error": "Tally integration is disabled in settings",
            }

        if not ledger_name:
            return {"success": False, "error": "ledger_name is required"}

        if not company:
            company = settings.erpnext_company or frappe.defaults.get_user_default("Company")

        # Resolve Tally company
        tally_company = (
            get_tally_company_for_erpnext_company(company) or settings.tally_company_name
        )
        if not tally_company:
            return {
                "success": False,
                "error": f"No Tally company mapped for ERPNext company '{company}'",
            }

        # 1. If ledger already exists, do nothing
        existing = check_ledger_exists(ledger_name)
        if existing.get("exists"):
            return {
                "success": True,
                "already_exists": True,
                "message": f"Ledger '{ledger_name}' already exists in Tally",
            }

        if not parent_group:
            return {
                "success": False,
                "error": f"Parent group not provided for ledger '{ledger_name}'",
            }

        # 2. Build XML
        xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Import Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <IMPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>All Masters</REPORTNAME>
    <STATICVARIABLES>
     <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
    </STATICVARIABLES>
   </REQUESTDESC>
   <REQUESTDATA>
    <TALLYMESSAGE xmlns:UDF="TallyUDF">
     <LEDGER NAME="{escape_xml(ledger_name)}" ACTION="Create">
      <NAME>{escape_xml(ledger_name)}</NAME>
      <PARENT>{escape_xml(parent_group)}</PARENT>
      <ISBILLWISEON>No</ISBILLWISEON>
      <ISCOSTCENTRESON>No</ISCOSTCENTRESON>
      <ISGSTAPPLICABLE>Applicable</ISGSTAPPLICABLE>
      <GSTTYPEOFSUPPLY>Goods</GSTTYPEOFSUPPLY>
     </LEDGER>
    </TALLYMESSAGE>
   </REQUESTDATA>
  </IMPORTDATA>
 </BODY>
</ENVELOPE>"""


        # 3. Log + send
        log = create_sync_log(
            operation_type="Create Generic Ledger",
            doctype_name="Account",
            doc_name=ledger_name,
            company=company,
            xml=xml_body,
        )
        result = send_xml_to_tally(log, xml_body)

        if not result.get("success"):
            return {
                "success": False,
                "error": result.get("error") or "Unknown error creating ledger",
                "error_type": result.get("error_type"),
                "log_id": log.name,
            }

        return {
            "success": True,
            "message": f"Ledger '{ledger_name}' created in Tally under '{parent_group}'",
            "log_id": log.name,
        }

    except Exception as e:
        msg = f"Exception creating generic ledger '{ledger_name}': {str(e)}"
        frappe.log_error("Tally Generic Ledger Creator", msg[:1000])
        return {"success": False, "error": str(e)}

# @frappe.whitelist()
# def create_sales_invoice_in_tally(invoice_name):
#     """
#     Create Sales Invoice voucher in Tally from ERPNext Sales Invoice.

#     Behaviour:
#     - Reference No = PO No
#     - Reference Date = PO Date
#     - Order No(s) = PO No
#     - Order Date = PO Date
#     """

#     try:
#         # ---------- 1. Load Sales Invoice ----------
#         inv = frappe.get_doc("Sales Invoice", invoice_name)

#         if inv.docstatus != 1:
#             return {
#                 "success": False,
#                 "error": "Sales Invoice must be submitted before syncing to Tally"
#             }

#         # Settings and Tally company
#         settings = get_settings()
#         tally_company = get_tally_company_for_erpnext_company(inv.company)
#         if not tally_company:
#             return {
#                 "success": False,
#                 "error": "No Tally company mapped for this ERPNext company"
#             }

#         # ---------- 2. Validate/Create Customer Ledger ----------
#         customer_name = inv.customer_name
#         customer_check = check_master_exists("Ledger", customer_name)
#         if not customer_check.get("exists"):
#             customer_result = create_customer_ledger_in_tally(inv.customer, inv.company)
#             if not customer_result.get("success"):
#                 return {
#                     "success": False,
#                     "error": (
#                         f"Customer ledger '{customer_name}' does not exist and "
#                         f"auto-creation failed: {customer_result.get('error')}"
#                     ),
#                     "retry_job": customer_result.get("retry_job")
#                 }

#         # ---------- 3. Validate Required Ledgers ----------
#         required_ledgers = {}

#         # Sales ledger
#         sales_ledger = settings.sales_ledger_name or "SALES A/C"
#         required_ledgers["Sales"] = sales_ledger

#         # GST ledgers
#         cgst_ledger = settings.cgst_ledger_name or "CGST"
#         sgst_ledger = settings.sgst_ledger_name or "SGST"
#         igst_ledger = settings.igst_ledger_name or "IGST"
#         required_ledgers["CGST"] = cgst_ledger
#         required_ledgers["SGST"] = sgst_ledger
#         required_ledgers["IGST"] = igst_ledger

#         # Round off ledger (field name: round_off_ledger_name)
#         round_off_ledger = settings.round_off_ledger_name or "Round Off"
#         required_ledgers["Round Off"] = round_off_ledger

#         missing_ledgers = []
#         for ledger_type, ledger_name in required_ledgers.items():
#             ledger_check = check_master_exists("Ledger", ledger_name)
#             if not ledger_check.get("exists"):
#                 missing_ledgers.append(f"{ledger_type} ({ledger_name})")

#         if missing_ledgers:
#             error_msg = f"Missing ledgers in Tally: {', '.join(missing_ledgers)}"
#             retry_job = create_retry_job(
#                 document_type="Sales Invoice",
#                 document_name=invoice_name,
#                 operation="Create Sales Invoice",

#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#             }

#         # ---------- 4. Validate Stock Items ----------
#         missing_items = []
#         for item in inv.items:
#             item_check = check_master_exists("StockItem", item.item_name)
#             if not item_check.get("exists"):
#                 missing_items.append(item.item_name)

#         if missing_items:
#             error_msg = f"Missing stock items in Tally: {', '.join(missing_items[:5])}"
#             if len(missing_items) > 5:
#                 error_msg += f" and {len(missing_items) - 5} more"

#             retry_job = create_retry_job(
#                 document_type="Sales Invoice",
#                 document_name=invoice_name,
#                 operation="Create Sales Invoice",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#             }

#         # ---------- 5. Build Voucher XML ----------

#         # Helper function for quantity display
#         def qty_display(qty, uom, per_box=6):
#             base = abs(float(qty))
#             boxes = int(base // per_box)
#             if base == int(base):
#                 base_str = f"{int(base)}"
#             else:
#                 base_str = f"{base:.2f}"
#             if uom:
#                 return f" {base_str} {uom} =  {boxes} Box"
#             return f" {base_str} = {boxes} Box"

#         # Dates
#         invoice_date = format_date_for_tally(inv.posting_date)
#         po_date_str = format_date_for_tally(inv.po_date) if inv.po_date else ""
#         lr_date_str = format_date_for_tally(inv.lr_date) if inv.lr_date else ""

#         # PO / expiry / destination
#         po_no = escape_xml(inv.po_no or "")
#         expiry_date_str = inv.custom_expiry_date or "" if hasattr(inv, "custom_expiry_date") else ""
#         expiry_ref = f"Expiry Date: {expiry_date_str}" if expiry_date_str else ""

#         place_of_supply = inv.place_of_supply or "India"
#         if "-" in place_of_supply:
#             state_name = place_of_supply.split("-")[1].strip()
#         else:
#             state_name = place_of_supply

#         destination = state_name
#         transporter_name = escape_xml(inv.transporter_name or "")
#         payment_terms = "30 Days"

#         # Totals
#         total_igst = total_cgst = total_sgst = 0.0
#         for tax_line in inv.taxes:
#             gst_type = (tax_line.gst_tax_type or "").lower()
#             tax_amount = float(tax_line.tax_amount or 0)
#             if gst_type == "igst":
#                 total_igst += tax_amount
#             elif gst_type == "cgst":
#                 total_cgst += tax_amount
#             elif gst_type == "sgst":
#                 total_sgst += tax_amount

#         total_igst = round(total_igst, 2)
#         total_cgst = round(total_cgst, 2)
#         total_sgst = round(total_sgst, 2)
#         grand_total = round(float(inv.base_rounded_total or inv.grand_total or 0), 2)
#         roundoff = float(inv.rounding_adjustment or 0)

#         # Interstate
#         interstate = False
#         company_gstin = inv.company_gstin or ""
#         customer_gstin = inv.billing_address_gstin or inv.customer_gstin or ""
#         if customer_gstin and company_gstin:
#             interstate = (customer_gstin[:2] != company_gstin[:2])

#         # Billing address
#         addr_lines = ""
#         try:
#             if inv.customer_address:
#                 billing_addr = frappe.get_doc("Address", inv.customer_address).as_dict()
#                 if billing_addr.get("address_line1"):
#                     addr_lines += f"\n       <ADDRESS>{escape_xml(billing_addr['address_line1'])}</ADDRESS>"
#                 if billing_addr.get("address_line2"):
#                     addr_lines += f"\n       <ADDRESS>{escape_xml(billing_addr['address_line2'])}</ADDRESS>"

#                 city_line = ""
#                 if billing_addr.get("city"):
#                     city_line = billing_addr["city"]
#                 if billing_addr.get("state"):
#                     city_line += f", {billing_addr['state']}" if city_line else billing_addr["state"]
#                 if billing_addr.get("pincode"):
#                     city_line += f" - {billing_addr['pincode']}"
#                 if city_line:
#                     addr_lines += f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"
#         except Exception:
#             pass

#         # Consignee (shipping) address
#         consignee_name = customer_name
#         consignee_lines = addr_lines
#         consignee_state = state_name
#         consignee_country = "India"
#         consignee_gstin = customer_gstin

#         try:
#             if hasattr(inv, "shipping_address_name") and inv.shipping_address_name:
#                 if inv.shipping_address_name != inv.customer_address:
#                     ship_addr = frappe.get_doc("Address", inv.shipping_address_name).as_dict()
#                     consignee_name = ship_addr.get("address_title") or customer_name
#                     consignee_lines = ""
#                     if ship_addr.get("address_line1"):
#                         consignee_lines += f"\n       <ADDRESS>{escape_xml(ship_addr['address_line1'])}</ADDRESS>"
#                     if ship_addr.get("address_line2"):
#                         consignee_lines += f"\n       <ADDRESS>{escape_xml(ship_addr['address_line2'])}</ADDRESS>"

#                     city_line = ""
#                     if ship_addr.get("city"):
#                         city_line = ship_addr["city"]
#                     if ship_addr.get("state"):
#                         city_line += f", {ship_addr['state']}" if city_line else ship_addr["state"]
#                         consignee_state = ship_addr.get("state") or state_name
#                     if ship_addr.get("pincode"):
#                         city_line += f" - {ship_addr['pincode']}"
#                     if city_line:
#                         consignee_lines += f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"

#                     consignee_country = ship_addr.get("country") or "India"
#                     if ship_addr.get("gstin"):
#                         consignee_gstin = ship_addr.get("gstin")
#         except Exception as e:
#             frappe.log_error(f"Error building consignee address: {str(e)}", "Tally Consignee")

#         # Items XML
#         items_xml = ""
#         for item in inv.items:
#             stock_group = item.item_group or "Primary"

#             qty = float(item.qty or 0)
#             qty_str = qty_display(qty, item.uom, per_box=6)

#             cgst_rate = int(round(getattr(item, "cgst_rate", 0) or 0))
#             sgst_rate = int(round(getattr(item, "sgst_rate", 0) or 0))
#             igst_rate = int(round(getattr(item, "igst_rate", 0) or 0))

#             item_mrp_text = ""
#             try:
#                 mrp_value = int(
#                     frappe.db.get_value("Item", {"item_name": item.item_name}, "custom_mrp") or 0
#                 )
#                 if mrp_value:
#                     item_mrp_text = f"MRP {mrp_value}"
#             except Exception:
#                 pass

#             mrp_xml = ""
#             if item_mrp_text:
#                 mrp_xml = f"""
#       <BASICUSERDESCRIPTION.LIST TYPE="String">
#         <BASICUSERDESCRIPTION>{item_mrp_text}</BASICUSERDESCRIPTION>
#        </BASICUSERDESCRIPTION.LIST>"""

#             items_xml += f"""
#       <ALLINVENTORYENTRIES.LIST>{mrp_xml}
#        <STOCKITEMNAME>{escape_xml(item.item_name)}</STOCKITEMNAME>
#        <GSTOVRDNCLASSIFICATION>{escape_xml(stock_group)}</GSTOVRDNCLASSIFICATION>
#        <GSTOVRDNINELIGIBLEITC> Not Applicable</GSTOVRDNINELIGIBLEITC>
#        <GSTOVRDNISREVCHARGEAPPL> Not Applicable</GSTOVRDNISREVCHARGEAPPL>
#        <GSTOVRDNTAXABILITY>Taxable</GSTOVRDNTAXABILITY>
#        <GSTSOURCETYPE>Stock Group</GSTSOURCETYPE>
#        <HSNSOURCETYPE>Stock Group</HSNSOURCETYPE>
#        <GSTOVRDNTYPEOFSUPPLY>Goods</GSTOVRDNTYPEOFSUPPLY>
#        <GSTRATEINFERAPPLICABILITY>Use GST Classification</GSTRATEINFERAPPLICABILITY>
#        <GSTHSNINFERAPPLICABILITY>Use GST Classification</GSTHSNINFERAPPLICABILITY>
#        <HSNOVRDNCLASSIFICATION>{escape_xml(stock_group)}</HSNOVRDNCLASSIFICATION>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISGSTASSESSABLEVALUEOVERRIDDEN>No</ISGSTASSESSABLEVALUEOVERRIDDEN>
#        <RATE>{item.rate}/{item.uom}</RATE>
#        <AMOUNT>{item.amount}</AMOUNT>
#        <ACTUALQTY>{qty_str}</ACTUALQTY>
#        <BILLEDQTY>{qty_str}</BILLEDQTY>
#        <BATCHALLOCATIONS.LIST>
#         <GODOWNNAME>Main Location</GODOWNNAME>
#         <BATCHNAME>Primary Batch</BATCHNAME>
#         <AMOUNT>{item.amount}</AMOUNT>
#         <ACTUALQTY>{qty_str}</ACTUALQTY>
#         <BILLEDQTY>{qty_str}</BILLEDQTY>
#        </BATCHALLOCATIONS.LIST>
#        <ACCOUNTINGALLOCATIONS.LIST>
#         <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
#         <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#         <AMOUNT>{item.amount}</AMOUNT>
#        </ACCOUNTINGALLOCATIONS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>CGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {cgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>SGST/UTGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {sgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>IGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {igst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#       </ALLINVENTORYENTRIES.LIST>"""

#         party_amount = -1 * grand_total

#         # Main XML
#         xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#  <HEADER>
#   <TALLYREQUEST>Import Data</TALLYREQUEST>
#  </HEADER>
#  <BODY>
#   <IMPORTDATA>
#    <REQUESTDESC>
#     <REPORTNAME>Vouchers</REPORTNAME>
#     <STATICVARIABLES>
#      <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
#     </STATICVARIABLES>
#    </REQUESTDESC>
#    <REQUESTDATA>
#     <TALLYMESSAGE xmlns:UDF="TallyUDF">
#      <VOUCHER REMOTEID="" VCHKEY="" VCHTYPE="Sales" ACTION="Create" OBJVIEW="Invoice Voucher View">
#       <ADDRESS.LIST TYPE="String">
#        <ADDRESS>{escape_xml(customer_name)}</ADDRESS>{addr_lines}
#       </ADDRESS.LIST>
#       <BASICBUYERADDRESS.LIST TYPE="String">
#        <BASICBUYERADDRESS>{escape_xml(customer_name)}</BASICBUYERADDRESS>{addr_lines}
#       </BASICBUYERADDRESS.LIST>
#       <CONSIGNEEADDRESS.LIST TYPE="String">
#        <CONSIGNEEADDRESS>{escape_xml(consignee_name)}</CONSIGNEEADDRESS>{consignee_lines}
#       </CONSIGNEEADDRESS.LIST>
#       <CONSIGNEESTATENAME>{escape_xml(consignee_state)}</CONSIGNEESTATENAME>
#       <CONSIGNEECOUNTRYNAME>{escape_xml(consignee_country)}</CONSIGNEECOUNTRYNAME>
#       <CONSIGNEEGSTIN>{escape_xml(consignee_gstin)}</CONSIGNEEGSTIN>
#       <OLDAUDITENTRYIDS.LIST TYPE="Number">
#        <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
#       </OLDAUDITENTRYIDS.LIST>
#       <INVOICEORDERLIST.LIST>
#        <BASICORDERDATE>{po_date_str or invoice_date}</BASICORDERDATE>
#        <BASICPURCHASEORDERNO>{po_no}</BASICPURCHASEORDERNO>
#        <BASICOTHERREFERENCES>{escape_xml(expiry_ref)}</BASICOTHERREFERENCES>
#       </INVOICEORDERLIST.LIST>
#       <BASICFINALDESTINATION>{escape_xml(destination)}</BASICFINALDESTINATION>
#       <BASICORDERREF>{escape_xml(expiry_ref)}</BASICORDERREF>
#       <BASICDUEDATEOFPYMT>{payment_terms}</BASICDUEDATEOFPYMT>
#       <BASICSHIPPEDBY>{transporter_name}</BASICSHIPPEDBY>
#       <DATE>{invoice_date}</DATE>
#       <ISINVOICE>Yes</ISINVOICE>
#       <STATENAME>{escape_xml(state_name)}</STATENAME>
#       <COUNTRYOFRESIDENCE>India</COUNTRYOFRESIDENCE>
#       <PARTYGSTIN>{escape_xml(customer_gstin)}</PARTYGSTIN>
#       <PLACEOFSUPPLY>{escape_xml(state_name)}</PLACEOFSUPPLY>
#       <PARTYNAME>{escape_xml(customer_name)}</PARTYNAME>
#       <VOUCHERTYPENAME>Sales</VOUCHERTYPENAME>
#       <VOUCHERNUMBER>{escape_xml(inv.name)}</VOUCHERNUMBER>
#       <REFERENCE>{po_no}</REFERENCE>
#       <REFERENCEDATE>{po_date_str or invoice_date}</REFERENCEDATE>
#       <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
#       <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
#       <VCHGSTSTATUSISAPPLICABLE>Yes</VCHGSTSTATUSISAPPLICABLE>
#       {items_xml}
#       <LEDGERENTRIES.LIST>
#        <OLDAUDITENTRYIDS.LIST TYPE="Number">
#         <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
#        </OLDAUDITENTRYIDS.LIST>
#        <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>Yes</ISLASTDEEMEDPOSITIVE>
#        <AMOUNT>{party_amount:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         # Tax ledgers
#         if not interstate:
#             if total_cgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{total_cgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{total_cgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#             if total_sgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{total_sgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{total_sgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#         else:
#             if total_igst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{total_igst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{total_igst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         # Round off
#         if abs(roundoff) >= 0.01:
#             roundoff_sign = "No" if roundoff > 0 else "Yes"
#             xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
#        <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>{roundoff_sign}</ISLASTDEEMEDPOSITIVE>
#        <ROUNDLIMIT> 1</ROUNDLIMIT>
#        <AMOUNT>{roundoff:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{roundoff:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         # Close XML
#         xml_body += """
#      </VOUCHER>
#     </TALLYMESSAGE>
#    </REQUESTDATA>
#   </IMPORTDATA>
#  </BODY>
# </ENVELOPE>"""

#         # ---------- 6. Log and Send ----------
#         log = create_sync_log(
#             operation_type="Create Sales Invoice",
#             doctype_name="Sales Invoice",
#             doc_name=invoice_name,
#             company=inv.company,
#             xml=xml_body,
#         )
#         result = send_xml_to_tally(log, xml_body)

#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Sales Invoice",
#                     document_name=invoice_name,
#                     operation="Create Sales Invoice",
#                     error_message=result.get("error"),
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None,
#                 }
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name,
#             }

#         # ---------- 7. Extract voucher number ----------
#         voucher_number = inv.name
#         response_text = result.get("response", "")
#         vch_start = response_text.find("<VOUCHERNUMBER>")
#         if vch_start != -1:
#             vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
#             if vch_end != -1:
#                 voucher_number = response_text[vch_start + 15 : vch_end].strip()

#         try:
#             inv.db_set("custom_posted_to_tally", 1, update_modified=False)
#             inv.db_set("custom_tally_voucher_number", voucher_number, update_modified=False)
#             inv.db_set("custom_tally_push_status", "Success", update_modified=False)
#             inv.db_set("custom_tally_sync_date", now(), update_modified=False)
#             frappe.db.commit()
#         except Exception:
#             pass

#         return {
#             "success": True,
#             "message": f"Sales Invoice '{inv.name}' created in Tally",
#             "voucher_number": voucher_number,
#             "sync_log": log.name,
#         }

#     except Exception as e:
#         error_msg = f"Exception creating sales invoice: {str(e)}"
#         frappe.log_error(error_msg, "Tally Sales Invoice Creator")
#         retry_job = create_retry_job(
#             document_type="Sales Invoice",
#             document_name=invoice_name,
#             operation="Create Sales Invoice",
#             error_message=error_msg,
#         )
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None,
#         }

# @frappe.whitelist()
# def create_sales_invoice_in_tally(invoice_name):
#     """
#     Create Sales Invoice voucher in Tally from ERPNext Sales Invoice.

#     Behaviour:
#     - Reference No = PO No
#     - Reference Date = PO Date
#     - Order No(s) = PO No
#     - Order Date = PO Date
#     """

#     try:
#         # ---------- 1. Load Sales Invoice ----------
#         inv = frappe.get_doc("Sales Invoice", invoice_name)

#         if inv.docstatus != 1:
#             return {
#                 "success": False,
#                 "error": "Sales Invoice must be submitted before syncing to Tally",
#             }

#         # ---------- 1.a Settings and Tally company ----------
#         settings = get_settings()
#         if not settings.enabled:
#             return {
#                 "success": False,
#                 "error": "Tally integration is disabled in settings",
#             }

#         # Use mapping helper if you have it; otherwise fall back to settings.tally_company_name
#         tally_company = get_tally_company_for_erpnext_company(inv.company)
#         if not tally_company:
#             tally_company = settings.tally_company_name

#         if not tally_company:
#             return {
#                 "success": False,
#                 "error": "No Tally company mapped for this ERPNext company",
#             }

#         # ---------- 2. Validate/Create Customer Ledger ----------
#         customer_name = inv.customer_name
#         customer_check = check_master_exists("Ledger", customer_name)
#         if not customer_check.get("exists"):
#             customer_result = create_customer_ledger_in_tally(inv.customer, inv.company)
#             if not customer_result.get("success"):
#                 return {
#                     "success": False,
#                     "error": (
#                         f"Customer ledger '{customer_name}' does not exist and "
#                         f"auto-creation failed: {customer_result.get('error')}"
#                     ),
#                     "retry_job": customer_result.get("retry_job"),
#                 }

#         # ---------- 3. Validate Required Ledgers ----------
#         required_ledgers = {}

#         sales_ledger = settings.sales_ledger_name or "SALES A/C"
#         required_ledgers["Sales"] = sales_ledger

#         cgst_ledger = settings.cgst_ledger_name or "CGST"
#         sgst_ledger = settings.sgst_ledger_name or "SGST"
#         igst_ledger = settings.igst_ledger_name or "IGST"
#         required_ledgers["CGST"] = cgst_ledger
#         required_ledgers["SGST"] = sgst_ledger
#         required_ledgers["IGST"] = igst_ledger

#         round_off_ledger = settings.round_off_ledger_name or "Round Off"
#         required_ledgers["Round Off"] = round_off_ledger

#         missing_ledgers = []
#         for ledger_type, ledger_name in required_ledgers.items():
#             ledger_check = check_master_exists("Ledger", ledger_name)
#             if not ledger_check.get("exists"):
#                 missing_ledgers.append(f"{ledger_type} ({ledger_name})")

#         if missing_ledgers:
#             error_msg = f"Missing ledgers in Tally: {', '.join(missing_ledgers)}"
#             retry_job = create_retry_job(
#                 document_type="Sales Invoice",
#                 document_name=invoice_name,
#                 operation="Create Sales Invoice",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#             }

#         # ---------- 4. Validate Stock Items ----------
#         missing_items = []
#         for item in inv.items:
#             item_check = check_master_exists("StockItem", item.item_name)
#             if not item_check.get("exists"):
#                 missing_items.append(item.item_name)

#         if missing_items:
#             error_msg = f"Missing stock items in Tally: {', '.join(missing_items[:5])}"
#             if len(missing_items) > 5:
#                 error_msg += f" and {len(missing_items) - 5} more"

#             retry_job = create_retry_job(
#                 document_type="Sales Invoice",
#                 document_name=invoice_name,
#                 operation="Create Sales Invoice",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#             }

#         # ---------- 5. Build Voucher XML ----------

#         def qty_display(qty, uom, per_box=6):
#             base = abs(float(qty))
#             boxes = int(base // per_box)
#             if base == int(base):
#                 base_str = f"{int(base)}"
#             else:
#                 base_str = f"{base:.2f}"
#             if uom:
#                 return f" {base_str} {uom} = {boxes} Box"
#             return f" {base_str} = {boxes} Box"

#         invoice_date = format_date_for_tally(inv.posting_date)
#         po_date_str = format_date_for_tally(inv.po_date) if inv.po_date else ""
#         lr_date_str = format_date_for_tally(inv.lr_date) if inv.lr_date else ""

#         po_no = escape_xml(inv.po_no or "")
#         expiry_date_str = (
#             inv.custom_expiry_date or "" if hasattr(inv, "custom_expiry_date") else ""
#         )
#         expiry_ref = f"Expiry Date: {expiry_date_str}" if expiry_date_str else ""

#         place_of_supply = inv.place_of_supply or "India"
#         if "-" in place_of_supply:
#             state_name = place_of_supply.split("-")[1].strip()
#         else:
#             state_name = place_of_supply

#         destination = state_name
#         transporter_name = escape_xml(inv.transporter_name or "")
#         payment_terms = "30 Days"

#         total_igst = total_cgst = total_sgst = 0.0
#         for tax_line in inv.taxes:
#             gst_type = (tax_line.gst_tax_type or "").lower()
#             tax_amount = float(tax_line.tax_amount or 0)
#             if gst_type == "igst":
#                 total_igst += tax_amount
#             elif gst_type == "cgst":
#                 total_cgst += tax_amount
#             elif gst_type == "sgst":
#                 total_sgst += tax_amount

#         total_igst = round(total_igst, 2)
#         total_cgst = round(total_cgst, 2)
#         total_sgst = round(total_sgst, 2)
#         grand_total = round(float(inv.base_rounded_total or inv.grand_total or 0), 2)
#         roundoff = float(inv.rounding_adjustment or 0)

#         interstate = False
#         company_gstin = inv.company_gstin or ""
#         customer_gstin = inv.billing_address_gstin or inv.customer_gstin or ""
#         if customer_gstin and company_gstin:
#             interstate = customer_gstin[:2] != company_gstin[:2]

#         addr_lines = ""
#         try:
#             if inv.customer_address:
#                 billing_addr = frappe.get_doc("Address", inv.customer_address).as_dict()
#                 if billing_addr.get("address_line1"):
#                     addr_lines += (
#                         f"\n       <ADDRESS>"
#                         f"{escape_xml(billing_addr['address_line1'])}</ADDRESS>"
#                     )
#                 if billing_addr.get("address_line2"):
#                     addr_lines += (
#                         f"\n       <ADDRESS>"
#                         f"{escape_xml(billing_addr['address_line2'])}</ADDRESS>"
#                     )

#                 city_line = ""
#                 if billing_addr.get("city"):
#                     city_line = billing_addr["city"]
#                 if billing_addr.get("state"):
#                     city_line += (
#                         f", {billing_addr['state']}"
#                         if city_line
#                         else billing_addr["state"]
#                     )
#                 if billing_addr.get("pincode"):
#                     city_line += f" - {billing_addr['pincode']}"
#                 if city_line:
#                     addr_lines += (
#                         f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"
#                     )
#         except Exception:
#             pass

#         consignee_name = customer_name
#         consignee_lines = addr_lines
#         consignee_state = state_name
#         consignee_country = "India"
#         consignee_gstin = customer_gstin

#         try:
#             if hasattr(inv, "shipping_address_name") and inv.shipping_address_name:
#                 if inv.shipping_address_name != inv.customer_address:
#                     ship_addr = frappe.get_doc(
#                         "Address", inv.shipping_address_name
#                     ).as_dict()
#                     consignee_name = ship_addr.get("address_title") or customer_name
#                     consignee_lines = ""
#                     if ship_addr.get("address_line1"):
#                         consignee_lines += (
#                             f"\n       <ADDRESS>"
#                             f"{escape_xml(ship_addr['address_line1'])}</ADDRESS>"
#                         )
#                     if ship_addr.get("address_line2"):
#                         consignee_lines += (
#                             f"\n       <ADDRESS>"
#                             f"{escape_xml(ship_addr['address_line2'])}</ADDRESS>"
#                         )

#                     city_line = ""
#                     if ship_addr.get("city"):
#                         city_line = ship_addr["city"]
#                     if ship_addr.get("state"):
#                         city_line += (
#                             f", {ship_addr['state']}"
#                             if city_line
#                             else ship_addr["state"]
#                         )
#                         consignee_state = ship_addr.get("state") or state_name
#                     if ship_addr.get("pincode"):
#                         city_line += f" - {ship_addr['pincode']}"
#                     if city_line:
#                         consignee_lines += (
#                             f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"
#                         )

#                     consignee_country = ship_addr.get("country") or "India"
#                     if ship_addr.get("gstin"):
#                         consignee_gstin = ship_addr.get("gstin")
#         except Exception as e:
#             frappe.log_error("Tally Consignee", f"Error building consignee address: {str(e)}")

#         items_xml = ""
#         for item in inv.items:
#             stock_group = item.item_group or "Primary"

#             qty = float(item.qty or 0)
#             qty_str = qty_display(qty, item.uom, per_box=6)

#             cgst_rate = int(round(getattr(item, "cgst_rate", 0) or 0))
#             sgst_rate = int(round(getattr(item, "sgst_rate", 0) or 0))
#             igst_rate = int(round(getattr(item, "igst_rate", 0) or 0))

#             item_mrp_text = ""
#             try:
#                 mrp_value = int(
#                     frappe.db.get_value(
#                         "Item", {"item_name": item.item_name}, "custom_mrp"
#                     )
#                     or 0
#                 )
#                 if mrp_value:
#                     item_mrp_text = f"MRP {mrp_value}"
#             except Exception:
#                 pass

#             mrp_xml = ""
#             if item_mrp_text:
#                 mrp_xml = f"""
#       <BASICUSERDESCRIPTION.LIST TYPE="String">
#         <BASICUSERDESCRIPTION>{item_mrp_text}</BASICUSERDESCRIPTION>
#        </BASICUSERDESCRIPTION.LIST>"""

#             items_xml += f"""
#       <ALLINVENTORYENTRIES.LIST>{mrp_xml}
#        <STOCKITEMNAME>{escape_xml(item.item_name)}</STOCKITEMNAME>
#        <GSTOVRDNCLASSIFICATION>{escape_xml(stock_group)}</GSTOVRDNCLASSIFICATION>
#        <GSTOVRDNINELIGIBLEITC> Not Applicable</GSTOVRDNINELIGIBLEITC>
#        <GSTOVRDNISREVCHARGEAPPL> Not Applicable</GSTOVRDNISREVCHARGEAPPL>
#        <GSTOVRDNTAXABILITY>Taxable</GSTOVRDNTAXABILITY>
#        <GSTSOURCETYPE>Stock Group</GSTSOURCETYPE>
#        <HSNSOURCETYPE>Stock Group</HSNSOURCETYPE>
#        <GSTOVRDNTYPEOFSUPPLY>Goods</GSTOVRDNTYPEOFSUPPLY>
#        <GSTRATEINFERAPPLICABILITY>Use GST Classification</GSTRATEINFERAPPLICABILITY>
#        <GSTHSNINFERAPPLICABILITY>Use GST Classification</GSTHSNINFERAPPLICABILITY>
#        <HSNOVRDNCLASSIFICATION>{escape_xml(stock_group)}</HSNOVRDNCLASSIFICATION>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISGSTASSESSABLEVALUEOVERRIDDEN>No</ISGSTASSESSABLEVALUEOVERRIDDEN>
#        <RATE>{item.rate}/{item.uom}</RATE>
#        <AMOUNT>{item.amount}</AMOUNT>
#        <ACTUALQTY>{qty_str}</ACTUALQTY>
#        <BILLEDQTY>{qty_str}</BILLEDQTY>
#        <BATCHALLOCATIONS.LIST>
#         <GODOWNNAME>Main Location</GODOWNNAME>
#         <BATCHNAME>Primary Batch</BATCHNAME>
#         <AMOUNT>{item.amount}</AMOUNT>
#         <ACTUALQTY>{qty_str}</ACTUALQTY>
#         <BILLEDQTY>{qty_str}</BILLEDQTY>
#        </BATCHALLOCATIONS.LIST>
#        <ACCOUNTINGALLOCATIONS.LIST>
#         <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
#         <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#         <AMOUNT>{item.amount}</AMOUNT>
#        </ACCOUNTINGALLOCATIONS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>CGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {cgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>SGST/UTGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {sgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>IGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {igst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#       </ALLINVENTORYENTRIES.LIST>"""

#         party_amount = -1 * grand_total

#         xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#  <HEADER>
#   <TALLYREQUEST>Import Data</TALLYREQUEST>
#  </HEADER>
#  <BODY>
#   <IMPORTDATA>
#    <REQUESTDESC>
#     <REPORTNAME>Vouchers</REPORTNAME>
#     <STATICVARIABLES>
#      <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
#     </STATICVARIABLES>
#    </REQUESTDESC>
#    <REQUESTDATA>
#     <TALLYMESSAGE xmlns:UDF="TallyUDF">
#      <VOUCHER REMOTEID="" VCHKEY="" VCHTYPE="Sales" ACTION="Create" OBJVIEW="Invoice Voucher View">
#       <ADDRESS.LIST TYPE="String">
#        <ADDRESS>{escape_xml(customer_name)}</ADDRESS>{addr_lines}
#       </ADDRESS.LIST>
#       <BASICBUYERADDRESS.LIST TYPE="String">
#        <BASICBUYERADDRESS>{escape_xml(customer_name)}</BASICBUYERADDRESS>{addr_lines}
#       </BASICBUYERADDRESS.LIST>
#       <CONSIGNEEADDRESS.LIST TYPE="String">
#        <CONSIGNEEADDRESS>{escape_xml(consignee_name)}</CONSIGNEEADDRESS>{consignee_lines}
#       </CONSIGNEEADDRESS.LIST>
#       <CONSIGNEESTATENAME>{escape_xml(consignee_state)}</CONSIGNEESTATENAME>
#       <CONSIGNEECOUNTRYNAME>{escape_xml(consignee_country)}</CONSIGNEECOUNTRYNAME>
#       <CONSIGNEEGSTIN>{escape_xml(consignee_gstin)}</CONSIGNEEGSTIN>
#       <OLDAUDITENTRYIDS.LIST TYPE="Number">
#        <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
#       </OLDAUDITENTRYIDS.LIST>
#       <INVOICEORDERLIST.LIST>
#        <BASICORDERDATE>{po_date_str or invoice_date}</BASICORDERDATE>
#        <BASICPURCHASEORDERNO>{po_no}</BASICPURCHASEORDERNO>
#        <BASICOTHERREFERENCES>{escape_xml(expiry_ref)}</BASICOTHERREFERENCES>
#       </INVOICEORDERLIST.LIST>
#       <BASICFINALDESTINATION>{escape_xml(destination)}</BASICFINALDESTINATION>
#       <BASICORDERREF>{escape_xml(expiry_ref)}</BASICORDERREF>
#       <BASICDUEDATEOFPYMT>{payment_terms}</BASICDUEDATEOFPYMT>
#       <BASICSHIPPEDBY>{transporter_name}</BASICSHIPPEDBY>
#       <DATE>{invoice_date}</DATE>
#       <ISINVOICE>Yes</ISINVOICE>
#       <STATENAME>{escape_xml(state_name)}</STATENAME>
#       <COUNTRYOFRESIDENCE>India</COUNTRYOFRESIDENCE>
#       <PARTYGSTIN>{escape_xml(customer_gstin)}</PARTYGSTIN>
#       <PLACEOFSUPPLY>{escape_xml(state_name)}</PLACEOFSUPPLY>
#       <PARTYNAME>{escape_xml(customer_name)}</PARTYNAME>
#       <VOUCHERTYPENAME>Sales</VOUCHERTYPENAME>
#       <VOUCHERNUMBER>{escape_xml(inv.name)}</VOUCHERNUMBER>
#       <REFERENCE>{po_no}</REFERENCE>
#       <REFERENCEDATE>{po_date_str or invoice_date}</REFERENCEDATE>
#       <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
#       <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
#       <VCHGSTSTATUSISAPPLICABLE>Yes</VCHGSTSTATUSISAPPLICABLE>
#       {items_xml}
#       <LEDGERENTRIES.LIST>
#        <OLDAUDITENTRYIDS.LIST TYPE="Number">
#         <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
#        </OLDAUDITENTRYIDS.LIST>
#        <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>Yes</ISLASTDEEMEDPOSITIVE>
#        <AMOUNT>{party_amount:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         if not interstate:
#             if total_cgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{total_cgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{total_cgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#             if total_sgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{total_sgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{total_sgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#         else:
#             if total_igst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{total_igst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{total_igst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         if abs(roundoff) >= 0.01:
#             roundoff_sign = "No" if roundoff > 0 else "Yes"
#             xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
#        <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>{roundoff_sign}</ISLASTDEEMEDPOSITIVE>
#        <ROUNDLIMIT> 1</ROUNDLIMIT>
#        <AMOUNT>{roundoff:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{roundoff:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         xml_body += """
#      </VOUCHER>
#     </TALLYMESSAGE>
#    </REQUESTDATA>
#   </IMPORTDATA>
#  </BODY>
# </ENVELOPE>"""

#         # ---------- 6. Log and Send ----------
#         log = create_sync_log(
#             operation_type="Create Sales Invoice",
#             doctype_name="Sales Invoice",
#             doc_name=invoice_name,
#             company=inv.company,
#             xml=xml_body,
#         )
#         result = send_xml_to_tally(log, xml_body)

#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Sales Invoice",
#                     document_name=invoice_name,
#                     operation="Create Sales Invoice",
#                     error_message=result.get("error"),
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None,
#                 }
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name,
#             }

#         # ---------- 7. Extract voucher number ----------
#         voucher_number = inv.name
#         response_text = result.get("response", "") or ""
#         vch_start = response_text.find("<VOUCHERNUMBER>")
#         if vch_start != -1:
#             vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
#             if vch_end != -1:
#                 voucher_number = response_text[vch_start + 15 : vch_end].strip()

#         try:
#             inv.db_set("custom_posted_to_tally", 1, update_modified=False)
#             inv.db_set(
#                 "custom_tally_voucher_number", voucher_number, update_modified=False
#             )
#             inv.db_set("custom_tally_push_status", "Success", update_modified=False)
#             inv.db_set("custom_tally_sync_date", now(), update_modified=False)
#             frappe.db.commit()
#         except Exception:
#             pass

#         return {
#             "success": True,
#             "message": f"Sales Invoice '{inv.name}' created in Tally",
#             "voucher_number": voucher_number,
#             "sync_log": log.name,
#         }

#     except Exception as e:
#         error_msg = f"Exception creating sales invoice: {str(e)}"
#         # short title, long message to avoid length errors
#         frappe.log_error("Tally Sales Invoice Creator", error_msg[:1000])
#         retry_job = create_retry_job(
#             document_type="Sales Invoice",
#             document_name=invoice_name,
#             operation="Create Sales Invoice",
#             error_message=error_msg,
#         )
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None,
#         }

def get_uom_map(item_doc):
    """Return {uom: factor} from Item.uoms (ERPNext standard child table)."""
    uom_map = {}
    for row in getattr(item_doc, "uoms", []) or []:
        if row.uom and row.conversion_factor:
            uom_map[row.uom] = float(row.conversion_factor)
    return uom_map


def qty_display_for_item(item_row, item_doc):
    """
    Build Tally-style quantity string using item UOM conversions.
    - item_row: Sales Invoice Item row
    - item_doc: Item master (frappe.get_doc("Item", item_row.item_code))
    """
    qty = abs(float(item_row.qty or 0))
    if not qty:
        return ""

    base_uom = item_row.uom or item_row.stock_uom or "Pcs"
    uom_map = get_uom_map(item_doc)

    base_factor = uom_map.get(base_uom, 1.0)

    # Try to find a 'Box' UOM
    box_uom = None
    box_factor = None
    for uom, factor in uom_map.items():
        if uom.lower() == "box":
            box_uom = uom
            box_factor = factor
            break

    # No Box defined → just "qty UOM"
    if not box_uom or not box_factor:
        # Normalise integer vs decimal
        if qty == int(qty):
            qty_str = f"{int(qty)}"
        else:
            qty_str = f"{qty:.2f}"
        return f" {qty_str} {base_uom}"

    # Convert base qty to 'Box' using factors
    # Example: base_uom=Pcs (1), Box=12 → boxes = qty / 12
    # If base_uom is Box and Box factor=12, then pcs = qty * 12
    if base_factor == 0:
        base_factor = 1.0

    # Compute quantity in Pcs as common base
    pcs_per_base = base_factor if base_uom.lower() == "pcs" else 1.0
    if base_uom.lower() == "pcs":
        pcs_qty = qty
    else:
        # qty (in base_uom) → pcs using conversion factors
        pcs_qty = qty * (pcs_per_base / base_factor) if pcs_per_base else qty

    boxes = pcs_qty // box_factor

    # Formatting
    if pcs_qty == int(pcs_qty):
        pcs_str = f"{int(pcs_qty)}"
    else:
        pcs_str = f"{pcs_qty:.2f}"

    # Boxes formatting, keep .2f only when needed
    if boxes == int(boxes):
        box_str = f"{int(boxes)}"
    else:
        box_str = f"{boxes:.2f}"

    return f" {pcs_str} Pcs = {box_str} Box"
    
@frappe.whitelist()
def create_sales_invoice_in_tally(invoice_name):
    """
    Create Sales Invoice voucher in Tally from ERPNext Sales Invoice.

    Behaviour:
    - Reference No = PO No
    - Reference Date = PO Date
    - Order No(s) = PO No
    - Order Date = PO Date
    """
    from tally_connect.tally_integration.api.validators import create_missing_masters_for_document


    try:
        # ---------- 1. Load Sales Invoice ----------
        inv = frappe.get_doc("Sales Invoice", invoice_name)

        if inv.docstatus != 1:
            return {
                "success": False,
                "error": "Sales Invoice must be submitted before syncing to Tally",
            }

        # ---------- 1.a Settings and Tally company ----------
        settings = get_settings()
        if not settings.enabled:
            return {
                "success": False,
                "error": "Tally integration is disabled in settings",
            }

        # Use mapping helper if you have it; otherwise fall back to settings.tally_company_name
        tally_company = get_tally_company_for_erpnext_company(inv.company)
        if not tally_company:
            tally_company = settings.tally_company_name

        if not tally_company:
            return {
                "success": False,
                "error": "No Tally company mapped for this ERPNext company",
            }

        # ---------- 2. Ensure all masters exist (customer, items, ledgers) ----------
        master_result = create_missing_masters_for_document("Sales Invoice", invoice_name)

        if not master_result.get("success"):
            error_msg = "Could not create required masters in Tally: " + "; ".join(
                master_result.get("errors") or []
            )
            retry_job = create_retry_job(
                document_type="Sales Invoice",
                document_name=invoice_name,
                operation="Create Sales Invoice",
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
                "created_masters": master_result.get("created", []),
            }

        # ---------- 3. Validate/Create Customer Ledger (safety net) ----------
        customer_name = inv.customer_name
        customer_check = check_master_exists("Ledger", customer_name)
        if not customer_check.get("exists"):
            customer_result = create_customer_ledger_in_tally(inv.customer, inv.company)
            if not customer_result.get("success"):
                return {
                    "success": False,
                    "error": (
                        f"Customer ledger '{customer_name}' does not exist and "
                        f"auto-creation failed: {customer_result.get('error')}"
                    ),
                    "retry_job": customer_result.get("retry_job"),
                }

        # ---------- 4. Resolve Ledgers (now assumed to exist) ----------
        required_ledgers = {}

        sales_ledger = settings.sales_ledger_name or "SALES A/C"
        required_ledgers["Sales"] = sales_ledger

        cgst_ledger = settings.cgst_ledger_name or "CGST"
        sgst_ledger = settings.sgst_ledger_name or "SGST"
        igst_ledger = settings.igst_ledger_name or "IGST"
        required_ledgers["CGST"] = cgst_ledger
        required_ledgers["SGST"] = sgst_ledger
        required_ledgers["IGST"] = igst_ledger

        round_off_ledger = settings.round_off_ledger_name or "Round Off"
        required_ledgers["Round Off"] = round_off_ledger

        # Optional sanity check: if any still missing, fail early
        missing_ledgers = []
        for ledger_type, ledger_name in required_ledgers.items():
            ledger_check = check_master_exists("Ledger", ledger_name)
            if not ledger_check.get("exists"):
                missing_ledgers.append(f"{ledger_type} ({ledger_name})")

        if missing_ledgers:
            error_msg = f"Missing ledgers in Tally even after master creation: {', '.join(missing_ledgers)}"
            retry_job = create_retry_job(
                document_type="Sales Invoice",
                document_name=invoice_name,
                operation="Create Sales Invoice",
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
            }

        # ---------- 5. Validate Stock Items (safety check, should already exist) ----------
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
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
            }

        # ---------- 6. Build Voucher XML ----------



        invoice_date = format_date_for_tally(inv.posting_date)
        po_date_str = format_date_for_tally(inv.po_date) if inv.po_date else ""
        lr_date_str = format_date_for_tally(inv.lr_date) if inv.lr_date else ""

        po_no = escape_xml(inv.po_no or "")
        expiry_date_str = (
            inv.custom_expiry_date or "" if hasattr(inv, "custom_expiry_date") else ""
        )
        expiry_ref = f"Expiry Date: {expiry_date_str}" if expiry_date_str else ""

        place_of_supply = inv.place_of_supply or "India"
        if "-" in place_of_supply:
            state_name = place_of_supply.split("-")[1].strip()
        else:
            state_name = place_of_supply

        destination = state_name
        transporter_name = escape_xml(inv.transporter_name or "")
        payment_terms = "30 Days"

        total_igst = total_cgst = total_sgst = 0.0
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

        interstate = False
        company_gstin = inv.company_gstin or ""
        customer_gstin = inv.billing_address_gstin or inv.customer_gstin or ""
        if customer_gstin and company_gstin:
            interstate = customer_gstin[:2] != company_gstin[:2]

        addr_lines = ""
        try:
            if inv.customer_address:
                billing_addr = frappe.get_doc("Address", inv.customer_address).as_dict()
                if billing_addr.get("address_line1"):
                    addr_lines += (
                        f"\n       <ADDRESS>"
                        f"{escape_xml(billing_addr['address_line1'])}</ADDRESS>"
                    )
                if billing_addr.get("address_line2"):
                    addr_lines += (
                        f"\n       <ADDRESS>"
                        f"{escape_xml(billing_addr['address_line2'])}</ADDRESS>"
                    )

                city_line = ""
                if billing_addr.get("city"):
                    city_line = billing_addr["city"]
                if billing_addr.get("state"):
                    city_line += (
                        f", {billing_addr['state']}"
                        if city_line
                        else billing_addr["state"]
                    )
                if billing_addr.get("pincode"):
                    city_line += f" - {billing_addr['pincode']}"
                if city_line:
                    addr_lines += (
                        f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"
                    )
        except Exception:
            pass

        consignee_name = customer_name
        consignee_lines = addr_lines
        consignee_state = state_name
        consignee_country = "India"
        consignee_gstin = customer_gstin

        try:
            if hasattr(inv, "shipping_address_name") and inv.shipping_address_name:
                if inv.shipping_address_name != inv.customer_address:
                    ship_addr = frappe.get_doc(
                        "Address", inv.shipping_address_name
                    ).as_dict()
                    consignee_name = ship_addr.get("address_title") or customer_name
                    consignee_lines = ""
                    if ship_addr.get("address_line1"):
                        consignee_lines += (
                            f"\n       <ADDRESS>"
                            f"{escape_xml(ship_addr['address_line1'])}</ADDRESS>"
                        )
                    if ship_addr.get("address_line2"):
                        consignee_lines += (
                            f"\n       <ADDRESS>"
                            f"{escape_xml(ship_addr['address_line2'])}</ADDRESS>"
                        )

                    city_line = ""
                    if ship_addr.get("city"):
                        city_line = ship_addr["city"]
                    if ship_addr.get("state"):
                        city_line += (
                            f", {ship_addr['state']}"
                            if city_line
                            else ship_addr["state"]
                        )
                        consignee_state = ship_addr.get("state") or state_name
                    if ship_addr.get("pincode"):
                        city_line += f" - {ship_addr['pincode']}"
                    if city_line:
                        consignee_lines += (
                            f"\n       <ADDRESS>{escape_xml(city_line)}</ADDRESS>"
                        )

                    consignee_country = ship_addr.get("country") or "India"
                    if ship_addr.get("gstin"):
                        consignee_gstin = ship_addr.get("gstin")
        except Exception as e:
            frappe.log_error("Tally Consignee", f"Error building consignee address: {str(e)}")

        items_xml = ""
        for item in inv.items:
            stock_group = item.item_group or "Primary"

            qty = float(item.qty or 0)
            qty_str = qty_display(qty, item.uom, per_box=6)

            cgst_rate = int(round(getattr(item, "cgst_rate", 0) or 0))
            sgst_rate = int(round(getattr(item, "sgst_rate", 0) or 0))
            igst_rate = int(round(getattr(item, "igst_rate", 0) or 0))

            item_mrp_text = ""
            try:
                mrp_value = int(
                    frappe.db.get_value(
                        "Item", {"item_name": item.item_name}, "custom_mrp"
                    )
                    or 0
                )
                if mrp_value:
                    item_mrp_text = f"MRP {mrp_value}"
            except Exception:
                pass

            mrp_xml = ""
            if item_mrp_text:
                mrp_xml = f"""
      <BASICUSERDESCRIPTION.LIST TYPE="String">
        <BASICUSERDESCRIPTION>{item_mrp_text}</BASICUSERDESCRIPTION>
       </BASICUSERDESCRIPTION.LIST>"""

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
      </ALLINVENTORYENTRIES.LIST>"""

        party_amount = -1 * grand_total

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
      <CONSIGNEEADDRESS.LIST TYPE="String">
       <CONSIGNEEADDRESS>{escape_xml(consignee_name)}</CONSIGNEEADDRESS>{consignee_lines}
      </CONSIGNEEADDRESS.LIST>
      <CONSIGNEESTATENAME>{escape_xml(consignee_state)}</CONSIGNEESTATENAME>
      <CONSIGNEECOUNTRYNAME>{escape_xml(consignee_country)}</CONSIGNEECOUNTRYNAME>
      <CONSIGNEEGSTIN>{escape_xml(consignee_gstin)}</CONSIGNEEGSTIN>
      <OLDAUDITENTRYIDS.LIST TYPE="Number">
       <OLDAUDITENTRYIDS>-1</OLDAUDITENTRYIDS>
      </OLDAUDITENTRYIDS.LIST>
      <INVOICEORDERLIST.LIST>
       <BASICORDERDATE>{po_date_str or invoice_date}</BASICORDERDATE>
       <BASICPURCHASEORDERNO>{po_no}</BASICPURCHASEORDERNO>
       <BASICOTHERREFERENCES>{escape_xml(expiry_ref)}</BASICOTHERREFERENCES>
      </INVOICEORDERLIST.LIST>
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
      <REFERENCE>{po_no}</REFERENCE>
      <REFERENCEDATE>{po_date_str or invoice_date}</REFERENCEDATE>
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

        if abs(roundoff) >= 0.01:
            roundoff_sign = "No" if roundoff > 0 else "Yes"
            xml_body += f"""
      <LEDGERENTRIES.LIST>
       <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
       <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <ISLASTDEEMEDPOSITIVE>{roundoff_sign}</ISLASTDEEMEDPOSITIVE>
       <ROUNDLIMIT> 1</ROUNDLIMIT>
       <AMOUNT>{roundoff:.2f}</AMOUNT>
       <VATEXPAMOUNT>{roundoff:.2f}</VATEXPAMOUNT>
      </LEDGERENTRIES.LIST>"""

        xml_body += """
     </VOUCHER>
    </TALLYMESSAGE>
   </REQUESTDATA>
  </IMPORTDATA>
 </BODY>
</ENVELOPE>"""

        # ---------- 7. Log and Send ----------
        log = create_sync_log(
            operation_type="Create Sales Invoice",
            doctype_name="Sales Invoice",
            doc_name=invoice_name,
            company=inv.company,
            xml=xml_body,
        )
        result = send_xml_to_tally(log, xml_body)

        if not result.get("success"):
            if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Sales Invoice",
                    document_name=invoice_name,
                    operation="Create Sales Invoice",
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

        # ---------- 8. Extract voucher number ----------
        voucher_number = inv.name
        response_text = result.get("response", "") or ""
        vch_start = response_text.find("<VOUCHERNUMBER>")
        if vch_start != -1:
            vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
            if vch_end != -1:
                voucher_number = response_text[vch_start + 15 : vch_end].strip()

        try:
            inv.db_set("custom_posted_to_tally", 1, update_modified=False)
            inv.db_set(
                "custom_tally_voucher_number", voucher_number, update_modified=False
            )
            inv.db_set("custom_tally_push_status", "Success", update_modified=False)
            inv.db_set("custom_tally_sync_date", now(), update_modified=False)
            frappe.db.commit()
        except Exception:
            pass

        return {
            "success": True,
            "message": f"Sales Invoice '{inv.name}' created in Tally",
            "voucher_number": voucher_number,
            "sync_log": log.name,
        }

    except Exception as e:
        error_msg = f"Exception creating sales invoice: {str(e)}"
        frappe.log_error("Tally Sales Invoice Creator", error_msg[:1000])
        retry_job = create_retry_job(
            document_type="Sales Invoice",
            document_name=invoice_name,
            operation="Create Sales Invoice",
            error_message=error_msg,
        )
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None,
        }

# @frappe.whitelist()
# def create_credit_note_in_tally(credit_note_name):
#     """
#     Create Credit Note voucher in Tally from ERPNext Sales Invoice (is_return=1).
    
#     Uses same master-creation logic as Sales Invoice.
#     """
    
#     try:
#         # Import validator locally to avoid circular import
#         from tally_connect.tally_integration.api.validators import (
#             create_missing_masters_for_document,
#         )
        
#         # ---------- 1. Load Credit Note ----------
#         cn = frappe.get_doc("Sales Invoice", credit_note_name)
        
#         if cn.docstatus != 1:
#             return {
#                 "success": False,
#                 "error": "Credit Note must be submitted before syncing to Tally",
#             }
        
#         if not cn.is_return:
#             return {
#                 "success": False,
#                 "error": "This is not a Credit Note (is_return is not set)",
#             }
        
#         # ---------- 1.a Settings and Tally company ----------
#         settings = get_settings()
#         if not settings.enabled:
#             return {
#                 "success": False,
#                 "error": "Tally integration is disabled in settings",
#             }
        
#         tally_company = get_tally_company_for_erpnext_company(cn.company)
#         if not tally_company:
#             tally_company = settings.tally_company_name
        
#         if not tally_company:
#             return {
#                 "success": False,
#                 "error": "No Tally company mapped for this ERPNext company",
#             }
        
#         # ---------- 2. Ensure all masters exist ----------
#         master_result = create_missing_masters_for_document("Sales Invoice", credit_note_name)
        
#         if not master_result.get("success"):
#             error_msg = "Could not create required masters in Tally: " + "; ".join(
#                 master_result.get("errors") or []
#             )
#             retry_job = create_retry_job(
#                 document_type="Credit Note",
#                 document_name=credit_note_name,
#                 operation="Create Credit Note",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#                 "created_masters": master_result.get("created", []),
#             }
        
#         # ---------- 3. Resolve Ledgers ----------
#         sales_ledger = settings.sales_ledger_name or "SALES A/C"
#         cgst_ledger = settings.cgst_ledger_name or "CGST"
#         sgst_ledger = settings.sgst_ledger_name or "SGST"
#         igst_ledger = settings.igst_ledger_name or "IGST"
#         round_off_ledger = settings.round_off_ledger_name or "Round Off"
        
#         # ---------- 4. Build Credit Note XML ----------
        
#         def qty_display(qty, uom, per_box=6):
#             base = abs(float(qty))
#             boxes = int(base // per_box)
#             if base == int(base):
#                 base_str = f"{int(base)}"
#             else:
#                 base_str = f"{base:.2f}"
#             if uom:
#                 return f" {base_str} {uom} = {boxes} Box"
#             return f" {base_str} = {boxes} Box"
        
#         cn_date = format_date_for_tally(cn.posting_date)
        
#         customer_name = cn.customer_name
        
#         place_of_supply = cn.place_of_supply or "India"
#         if "-" in place_of_supply:
#             state_name = place_of_supply.split("-")[1].strip()
#         else:
#             state_name = place_of_supply
        
#         # Tax totals
#         total_igst = total_cgst = total_sgst = 0.0
#         for tax_line in cn.taxes:
#             gst_type = (tax_line.gst_tax_type or "").lower()
#             tax_amount = float(tax_line.tax_amount or 0)
#             if gst_type == "igst":
#                 total_igst += tax_amount
#             elif gst_type == "cgst":
#                 total_cgst += tax_amount
#             elif gst_type == "sgst":
#                 total_sgst += tax_amount
        
#         total_igst = round(total_igst, 2)
#         total_cgst = round(total_cgst, 2)
#         total_sgst = round(total_sgst, 2)
#         grand_total = round(float(cn.base_rounded_total or cn.grand_total or 0), 2)
#         roundoff = float(cn.rounding_adjustment or 0)
        
#         interstate = False
#         company_gstin = cn.company_gstin or ""
#         customer_gstin = cn.billing_address_gstin or cn.customer_gstin or ""
#         if customer_gstin and company_gstin:
#             interstate = customer_gstin[:2] != company_gstin[:2]
        
#         # Items XML
#         items_xml = ""
#         for item in cn.items:
#             stock_group = item.item_group or "Primary"
            
#             qty = float(item.qty or 0)
#             qty_str = qty_display(qty, item.uom, per_box=6)
            
#             cgst_rate = int(round(getattr(item, "cgst_rate", 0) or 0))
#             sgst_rate = int(round(getattr(item, "sgst_rate", 0) or 0))
#             igst_rate = int(round(getattr(item, "igst_rate", 0) or 0))
            
#             items_xml += f"""
#       <ALLINVENTORYENTRIES.LIST>
#        <STOCKITEMNAME>{escape_xml(item.item_name)}</STOCKITEMNAME>
#        <GSTOVRDNCLASSIFICATION>{escape_xml(stock_group)}</GSTOVRDNCLASSIFICATION>
#        <GSTOVRDNINELIGIBLEITC> Not Applicable</GSTOVRDNINELIGIBLEITC>
#        <GSTOVRDNISREVCHARGEAPPL> Not Applicable</GSTOVRDNISREVCHARGEAPPL>
#        <GSTOVRDNTAXABILITY>Taxable</GSTOVRDNTAXABILITY>
#        <GSTSOURCETYPE>Stock Group</GSTSOURCETYPE>
#        <HSNSOURCETYPE>Stock Group</HSNSOURCETYPE>
#        <GSTOVRDNTYPEOFSUPPLY>Goods</GSTOVRDNTYPEOFSUPPLY>
#        <GSTRATEINFERAPPLICABILITY>Use GST Classification</GSTRATEINFERAPPLICABILITY>
#        <GSTHSNINFERAPPLICABILITY>Use GST Classification</GSTHSNINFERAPPLICABILITY>
#        <HSNOVRDNCLASSIFICATION>{escape_xml(stock_group)}</HSNOVRDNCLASSIFICATION>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISGSTASSESSABLEVALUEOVERRIDDEN>No</ISGSTASSESSABLEVALUEOVERRIDDEN>
#        <RATE>{item.rate}/{item.uom}</RATE>
#        <AMOUNT>{-1 * item.amount}</AMOUNT>
#        <ACTUALQTY>{qty_str}</ACTUALQTY>
#        <BILLEDQTY>{qty_str}</BILLEDQTY>
#        <BATCHALLOCATIONS.LIST>
#         <GODOWNNAME>Main Location</GODOWNNAME>
#         <BATCHNAME>Primary Batch</BATCHNAME>
#         <AMOUNT>{-1 * item.amount}</AMOUNT>
#         <ACTUALQTY>{qty_str}</ACTUALQTY>
#         <BILLEDQTY>{qty_str}</BILLEDQTY>
#        </BATCHALLOCATIONS.LIST>
#        <ACCOUNTINGALLOCATIONS.LIST>
#         <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
#         <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#         <AMOUNT>{-1 * item.amount}</AMOUNT>
#        </ACCOUNTINGALLOCATIONS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>CGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {cgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>SGST/UTGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {sgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>IGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {igst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#       </ALLINVENTORYENTRIES.LIST>"""
        
#         party_amount = grand_total  # Credit = positive for party
        
#         xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#  <HEADER>
#   <TALLYREQUEST>Import Data</TALLYREQUEST>
#  </HEADER>
#  <BODY>
#   <IMPORTDATA>
#    <REQUESTDESC>
#     <REPORTNAME>Vouchers</REPORTNAME>
#     <STATICVARIABLES>
#      <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
#     </STATICVARIABLES>
#    </REQUESTDESC>
#    <REQUESTDATA>
#     <TALLYMESSAGE xmlns:UDF="TallyUDF">
#      <VOUCHER REMOTEID="" VCHKEY="" VCHTYPE="Credit Note" ACTION="Create" OBJVIEW="Invoice Voucher View">
#       <DATE>{cn_date}</DATE>
#       <ISINVOICE>Yes</ISINVOICE>
#       <STATENAME>{escape_xml(state_name)}</STATENAME>
#       <COUNTRYOFRESIDENCE>India</COUNTRYOFRESIDENCE>
#       <PARTYGSTIN>{escape_xml(customer_gstin)}</PARTYGSTIN>
#       <PLACEOFSUPPLY>{escape_xml(state_name)}</PLACEOFSUPPLY>
#       <PARTYNAME>{escape_xml(customer_name)}</PARTYNAME>
#       <VOUCHERTYPENAME>Credit Note</VOUCHERTYPENAME>
#       <VOUCHERNUMBER>{escape_xml(cn.name)}</VOUCHERNUMBER>
#       <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
#       <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
#       <VCHGSTSTATUSISAPPLICABLE>Yes</VCHGSTSTATUSISAPPLICABLE>
#       {items_xml}
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>No</ISLASTDEEMEDPOSITIVE>
#        <AMOUNT>{-1 * party_amount:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""
        
#         # Tax ledgers (signs reversed for Credit Note)
#         if not interstate:
#             if total_cgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{-1 * total_cgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * total_cgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#             if total_sgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{-1 * total_sgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * total_sgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#         else:
#             if total_igst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{-1 * total_igst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * total_igst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
        
#         if abs(roundoff) >= 0.01:
#             roundoff_sign = "Yes" if roundoff > 0 else "No"
#             xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
#        <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>{roundoff_sign}</ISLASTDEEMEDPOSITIVE>
#        <ROUNDLIMIT> 1</ROUNDLIMIT>
#        <AMOUNT>{-1 * roundoff:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * roundoff:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
        
#         xml_body += """
#      </VOUCHER>
#     </TALLYMESSAGE>
#    </REQUESTDATA>
#   </IMPORTDATA>
#  </BODY>
# </ENVELOPE>"""
        
#         # ---------- 5. Log and Send ----------
#         log = create_sync_log(
#             operation_type="Create Credit Note",
#             doctype_name="Sales Invoice",
#             doc_name=credit_note_name,
#             company=cn.company,
#             xml=xml_body,
#         )
#         result = send_xml_to_tally(log, xml_body)
        
#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Credit Note",
#                     document_name=credit_note_name,
#                     operation="Create Credit Note",
#                     error_message=result.get("error"),
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None,
#                 }
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name,
#             }
        
#         # ---------- 6. Extract voucher number ----------
#         voucher_number = cn.name
#         response_text = result.get("response", "") or ""
#         vch_start = response_text.find("<VOUCHERNUMBER>")
#         if vch_start != -1:
#             vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
#             if vch_end != -1:
#                 voucher_number = response_text[vch_start + 15 : vch_end].strip()
        
#         try:
#             cn.db_set("custom_posted_to_tally", 1, update_modified=False)
#             cn.db_set("custom_tally_voucher_number", voucher_number, update_modified=False)
#             cn.db_set("custom_tally_push_status", "Success", update_modified=False)
#             cn.db_set("custom_tally_sync_date", now(), update_modified=False)
#             frappe.db.commit()
#         except Exception:
#             pass
        
#         return {
#             "success": True,
#             "message": f"Credit Note '{cn.name}' created in Tally",
#             "voucher_number": voucher_number,
#             "sync_log": log.name,
#         }
    
#     except Exception as e:
#         error_msg = f"Exception creating credit note: {str(e)}"
#         frappe.log_error("Tally Credit Note Creator", error_msg[:1000])
#         retry_job = create_retry_job(
#             document_type="Credit Note",
#             document_name=credit_note_name,
#             operation="Create Credit Note",
#             error_message=error_msg,
#         )
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None,
#         }

# @frappe.whitelist()
# def tally_create_credit_note(credit_note_name: str):
#     """
#     Create Credit Note in Tally from ERPNext Sales Invoice (is_return = 1).

#     Uses:
#     - Tally Integration Settings for voucher type, CGST/SGST/IGST/round‑off ledgers, default godown, etc.
#     - Field Mapping + Tax Ledger Mapping for sales / tax / party ledgers.
#     - Sales Invoice (return) as source document.
#     """
#     import json
#     from frappe.utils import now

#     # -------- 1. Load documents --------
#     cn = frappe.get_doc("Sales Invoice", credit_note_name)
#     if cn.docstatus != 1 or not cn.is_return:
#         frappe.throw("Credit Note must be a submitted Sales Invoice with 'Is Return' checked.")

#     settings = frappe.get_single("Tally Integration Settings")
#     company = frappe.get_doc("Company", cn.company)

#     # Tally company name
#     tally_company = getattr(company, "custom_tally_company_name", None) or settings.tally_company_name

#     # -------- 2. Resolve voucher type, groups & ledgers from settings --------
#     voucher_type = settings.credit_note_voucher_type or "Credit Note"
#     voucher_entry_mode = "Item Invoice"

#     # Sales ledger (from mapping table, then fallback)
#     sales_ledger = settings.sales_ledger_name or "SALES AC"

#     # Tax ledgers from GST section in settings
#     cgst_ledger = settings.cgst_ledger_name
#     sgst_ledger = settings.sgst_ledger_name
#     igst_ledger = settings.igst_ledger_name
#     round_off_ledger = settings.round_off_ledger_name 

#     # Default inventory settings
#     default_godown = settings.default_godown_name
#     default_batch = settings.default_batch_name
#     per_box = settings.case_size_units

#     # Party ledger: either mapping override or customer name itself
#     customer_ledger = cn.customer_name

#     # -------- 3. Compute GST amounts & round off from invoice taxes --------
#     total_cgst = 0.0
#     total_sgst = 0.0
#     total_igst = 0.0

#     for tax in cn.taxes:
#         gst_type = (tax.gst_tax_type or "").lower()
#         amt = abs(float(tax.tax_amount or 0))
#         if gst_type == "cgst":
#             total_cgst += amt
#         elif gst_type == "sgst":
#             total_sgst += amt
#         elif gst_type == "igst":
#             total_igst += amt

#     grand_total = abs(round(float(cn.base_rounded_total or cn.grand_total or 0), 2))
#     round_off = float(cn.rounding_adjustment or 0)

#     # -------- 4. Helper: qty display (same as local Tally sample) --------
#     def qty_display(qty, uom, per_box_val=6):
#         base = abs(float(qty or 0))
#         boxes = int(base // per_box_val)
#         if base == int(base):
#             base_str = f"{int(base)}"
#         else:
#             base_str = f"{base:.2f}"
#         if uom:
#             return f"{base_str} {uom} {boxes} Box"
#         return f"{base_str} {boxes} Box"

#     def convert_date(dateval):
#         if not dateval:
#             return ""
#         try:
#             return dateval.strftime("%Y%m%d")
#         except Exception:
#             return str(dateval).replace("-", "")

#     posting_date = convert_date(cn.posting_date)

#     # -------- 5. Build items XML (stock item, sales ledger) --------
#     items_xml_parts = []
#     for it in cn.items:
#         qty_txt = qty_display(it.qty, it.uom, per_box)
#         amount = abs(float(it.amount or 0))
#         rate = abs(float(it.rate or 0))

#         item_xml = f"""
#         <ALLINVENTORYENTRIES.LIST>
#             <STOCKITEMNAME>{frappe.utils.escape_html(it.item_name)}</STOCKITEMNAME>
#             <GSTOVRDNCLASSIFICATION>{frappe.utils.escape_html(it.item_group or "Primary")}</GSTOVRDNCLASSIFICATION>
#             <GSTOVRDNTAXABILITY>Taxable</GSTOVRDNTAXABILITY>
#             <GSTSOURCETYPE>Stock Group</GSTSOURCETYPE>
#             <HSNSOURCETYPE>Stock Group</HSNSOURCETYPE>
#             <GSTOVRDNTYPEOFSUPPLY>Goods</GSTOVRDNTYPEOFSUPPLY>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <RATE>{rate:.2f}{it.uom or "Pcs"}</RATE>
#             <AMOUNT>-{amount:.2f}</AMOUNT>
#             <ACTUALQTY>{qty_txt}</ACTUALQTY>
#             <BILLEDQTY>{qty_txt}</BILLEDQTY>
#             <BATCHALLOCATIONS.LIST>
#                 <GODOWNNAME>{frappe.utils.escape_html(default_godown)}</GODOWNNAME>
#                 <BATCHNAME>{frappe.utils.escape_html(default_batch)}</BATCHNAME>
#                 <AMOUNT>-{amount:.2f}</AMOUNT>
#                 <ACTUALQTY>{qty_txt}</ACTUALQTY>
#                 <BILLEDQTY>{qty_txt}</BILLEDQTY>
#             </BATCHALLOCATIONS.LIST>
#             <ACCOUNTINGALLOCATIONS.LIST>
#                 <LEDGERNAME>{frappe.utils.escape_html(sales_ledger)}</LEDGERNAME>
#                 <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#                 <LEDGERFROMITEM>No</LEDGERFROMITEM>
#                 <ISPARTYLEDGER>No</ISPARTYLEDGER>
#                 <AMOUNT>-{amount:.2f}</AMOUNT>
#             </ACCOUNTINGALLOCATIONS.LIST>
#         </ALLINVENTORYENTRIES.LIST>
#         """
#         items_xml_parts.append(item_xml)

#     items_xml = "\n".join(items_xml_parts)

#     # -------- 6. Header XML (voucher type, party ledger, etc.) --------
#     state_name = (cn.place_of_supply or "").split("-")[-1].strip() if cn.place_of_supply else ""
#     party_gstin = cn.billing_address_gstin or cn.customer_gstin or ""

#     party_ledger_xml = f"""
#     <LEDGERENTRIES.LIST>
#         <LEDGERNAME>{frappe.utils.escape_html(customer_ledger)}</LEDGERNAME>
#         <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#         <LEDGERFROMITEM>No</LEDGERFROMITEM>
#         <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
#         <AMOUNT>{grand_total:.2f}</AMOUNT>
#         <BILLALLOCATIONS.LIST>
#             <NAME>{frappe.utils.escape_html(cn.name)}</NAME>
#             <BILLTYPE>New Ref</BILLTYPE>
#             <AMOUNT>{grand_total:.2f}</AMOUNT>
#         </BILLALLOCATIONS.LIST>
#     </LEDGERENTRIES.LIST>
#     """

#     # -------- 7. Tax ledgers (CGST / SGST / IGST from settings) --------
#     tax_ledgers_xml = ""

#     if total_cgst:
#         tax_ledgers_xml += f"""
#         <LEDGERENTRIES.LIST>
#             <LEDGERNAME>{frappe.utils.escape_html(cgst_ledger)}</LEDGERNAME>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <LEDGERFROMITEM>No</LEDGERFROMITEM>
#             <ISPARTYLEDGER>No</ISPARTYLEDGER>
#             <AMOUNT>-{total_cgst:.2f}</AMOUNT>
#         </LEDGERENTRIES.LIST>
#         """

#     if total_sgst:
#         tax_ledgers_xml += f"""
#         <LEDGERENTRIES.LIST>
#             <LEDGERNAME>{frappe.utils.escape_html(sgst_ledger)}</LEDGERNAME>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <LEDGERFROMITEM>No</LEDGERFROMITEM>
#             <ISPARTYLEDGER>No</ISPARTYLEDGER>
#             <AMOUNT>-{total_sgst:.2f}</AMOUNT>
#         </LEDGERENTRIES.LIST>
#         """

#     if total_igst:
#         tax_ledgers_xml += f"""
#         <LEDGERENTRIES.LIST>
#             <LEDGERNAME>{frappe.utils.escape_html(igst_ledger)}</LEDGERNAME>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <LEDGERFROMITEM>No</LEDGERFROMITEM>
#             <ISPARTYLEDGER>No</ISPARTYLEDGER>
#             <AMOUNT>-{total_igst:.2f}</AMOUNT>
#         </LEDGERENTRIES.LIST>
#         """

#     # -------- 8. Round‑off ledger --------
#     round_off_xml = ""
#     if abs(round_off) >= 0.01:
#         roundoff_sign = "Yes" if round_off > 0 else "No"
#         round_off_xml = f"""
#         <LEDGERENTRIES.LIST>
#             <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
#             <LEDGERNAME>{frappe.utils.escape_html(round_off_ledger)}</LEDGERNAME>
#             <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
#             <LEDGERFROMITEM>No</LEDGERFROMITEM>
#             <ISPARTYLEDGER>No</ISPARTYLEDGER>
#             <AMOUNT>{round_off:.2f}</AMOUNT>
#         </LEDGERENTRIES.LIST>
#         """

#     # -------- 9. Final XML envelope --------
#     xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#   <HEADER>
#     <TALLYREQUEST>Import Data</TALLYREQUEST>
#   </HEADER>
#   <BODY>
#     <IMPORTDATA>
#       <REQUESTDESC>
#         <REPORTNAME>Vouchers</REPORTNAME>
#         <STATICVARIABLES>
#           <SVCURRENTCOMPANY>{frappe.utils.escape_html(tally_company)}</SVCURRENTCOMPANY>
#         </STATICVARIABLES>
#       </REQUESTDESC>
#       <REQUESTDATA>
#         <TALLYMESSAGE xmlns:UDF="TallyUDF">
#           <VOUCHER VCHTYPE="{voucher_type}" ACTION="Create" OBJVIEW="Invoice Voucher View"
#                    VCHENTRYMODE="{voucher_entry_mode}">
#             <DATE>{posting_date}</DATE>
#             <VOUCHERTYPENAME>{voucher_type}</VOUCHERTYPENAME>
#             <VOUCHERNUMBER>{frappe.utils.escape_html(cn.name)}</VOUCHERNUMBER>
#             <STATENAME>{frappe.utils.escape_html(state_name)}</STATENAME>
#             <COUNTRYOFRESIDENCE>India</COUNTRYOFRESIDENCE>
#             <PARTYGSTIN>{frappe.utils.escape_html(party_gstin)}</PARTYGSTIN>
#             <PLACEOFSUPPLY>{frappe.utils.escape_html(state_name)}</PLACEOFSUPPLY>
#             <PARTYNAME>{frappe.utils.escape_html(cn.customer_name)}</PARTYNAME>
#             <PARTYLEDGERNAME>{frappe.utils.escape_html(customer_ledger)}</PARTYLEDGERNAME>
#             <CMPGSTIN>{frappe.utils.escape_html(cn.company_gstin or "")}</CMPGSTIN>
#             <CMPGSTSTATE>{frappe.utils.escape_html(cn.company_address_state or "")}</CMPGSTSTATE>
#             <NARRATION>{frappe.utils.escape_html(cn.remarks or ("Credit Note " + cn.name))}</NARRATION>
#             <ISINVOICE>Yes</ISINVOICE>

#             {items_xml}
#             {party_ledger_xml}
#             {tax_ledgers_xml}
#             {round_off_xml}

#           </VOUCHER>
#         </TALLYMESSAGE>
#       </REQUESTDATA>
#     </IMPORTDATA>
#   </BODY>
# </ENVELOPE>
# """

#     # -------- 10. Call existing low‑level sender + log (reuse pattern from invoice) --------
#     from tally.api.utils import send_xml_to_tally, create_sync_log

#     log = create_sync_log(
#         refdoctype="Sales Invoice",
#         refdocname=cn.name,
#         operationtype="Create Credit Note",
#         synccategory="Voucher",
#         tallyobjecttype="Credit Note",
#         requestpayload=xml_body,
#         tallyurl=settings.tally_url,
#     )

#     success, response_text, error_msg = send_xml_to_tally(xml_body, settings.tally_url)

#     if success:
#         # mark invoice
#         cn.db_set("custom_posted_to_tally", 1, update_modified=False)
#         cn.db_set("custom_tally_voucher_number", cn.name, update_modified=False)
#         cn.db_set("custom_tally_push_status", "Success", update_modified=False)
#         frappe.db.set_value("Tally Sync Log", log.name, {
#             "syncstatus": "Success",
#             "tallyresponse": response_text[:65000],
#             "synctimestamp": now()
#         })
#         frappe.db.commit()
#         return {"success": True, "voucher_number": cn.name, "log_id": log.name}
#     else:
#         frappe.db.set_value("Tally Sync Log", log.name, {
#             "syncstatus": "Failed",
#             "errormessage": (error_msg or "")[:1000],
#             "tallyresponse": (response_text or "")[:65000],
#         })
#         frappe.db.commit()
#         return {"success": False, "error": error_msg or "Unknown error", "log_id": log.name}


# @frappe.whitelist()
# def create_credit_note_in_tally(credit_note_name):
#     """
#     Create Credit Note voucher in Tally from ERPNext Sales Invoice (is_return=1).

#     Behaviour:
#     - Uses same master-creation logic as Sales Invoice.
#     - Uses same qty_display style as create_sales_invoice_in_tally (Pcs + Box).
#     """
#     # Import here to avoid circular import
#     from tally_connect.tally_integration.api.validators import (
#         create_missing_masters_for_document,
#     )

#     try:
#         # ---------- 1. Load Credit Note ----------
#         cn = frappe.get_doc("Sales Invoice", credit_note_name)

#         if cn.docstatus != 1:
#             return {
#                 "success": False,
#                 "error": "Credit Note must be submitted before syncing to Tally",
#             }

#         if not cn.is_return:
#             return {
#                 "success": False,
#                 "error": "This is not a Credit Note (is_return is not set)",
#             }

#         # ---------- 1.a Settings and Tally company ----------
#         settings = get_settings()
#         if not settings.enabled:
#             return {
#                 "success": False,
#                 "error": "Tally integration is disabled in settings",
#             }

#         tally_company = get_tally_company_for_erpnext_company(cn.company)
#         if not tally_company:
#             tally_company = settings.tally_company_name

#         if not tally_company:
#             return {
#                 "success": False,
#                 "error": "No Tally company mapped for this ERPNext company",
#             }

#         # ---------- 2. Ensure all masters exist (same as Sales Invoice) ----------
#         master_result = create_missing_masters_for_document("Sales Invoice", credit_note_name)

#         if not master_result.get("success"):
#             error_msg = "Could not create required masters in Tally: " + "; ".join(
#                 master_result.get("errors") or []
#             )
#             retry_job = create_retry_job(
#                 document_type="Credit Note",
#                 document_name=credit_note_name,
#                 operation="Create Credit Note",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#                 "created_masters": master_result.get("created", []),
#             }

#         # ---------- 3. Resolve Ledgers ----------
#         sales_ledger = settings.sales_ledger_name or "SALES A/C"
#         cgst_ledger = settings.cgst_ledger_name or "CGST"
#         sgst_ledger = settings.sgst_ledger_name or "SGST"
#         igst_ledger = settings.igst_ledger_name or "IGST"
#         round_off_ledger = settings.round_off_ledger_name or "Round Off"

#         # ---------- 4. Local qty_display (SAME as in Sales Invoice) ----------

#         def qty_display(qty, uom, per_box=6):
#             """Display quantity same style as Sales Invoice: ' 12 Pcs = 2 Box'."""
#             base = abs(float(qty or 0))
#             boxes = int(base // per_box)
#             if base == int(base):
#                 base_str = f"{int(base)}"
#             else:
#                 base_str = f"{base:.2f}"
#             if uom:
#                 return f" {base_str} {uom} = {boxes} Box"
#             return f" {base_str} = {boxes} Box"

#         # ---------- 5. Core values ----------
#         cn_date = format_date_for_tally(cn.posting_date)
#         customer_name = cn.customer_name

#         place_of_supply = cn.place_of_supply or "India"
#         if "-" in place_of_supply:
#             state_name = place_of_supply.split("-")[1].strip()
#         else:
#             state_name = place_of_supply

#         # Tax totals
#         total_igst = total_cgst = total_sgst = 0.0
#         for tax_line in cn.taxes:
#             gst_type = (tax_line.gst_tax_type or "").lower()
#             tax_amount = float(tax_line.tax_amount or 0)
#             if gst_type == "igst":
#                 total_igst += tax_amount
#             elif gst_type == "cgst":
#                 total_cgst += tax_amount
#             elif gst_type == "sgst":
#                 total_sgst += tax_amount

#         total_igst = round(total_igst, 2)
#         total_cgst = round(total_cgst, 2)
#         total_sgst = round(total_sgst, 2)
#         grand_total = round(float(cn.base_rounded_total or cn.grand_total or 0), 2)
#         roundoff = float(cn.rounding_adjustment or 0)

#         interstate = False
#         company_gstin = cn.company_gstin or ""
#         customer_gstin = cn.billing_address_gstin or cn.customer_gstin or ""
#         if customer_gstin and company_gstin:
#             interstate = customer_gstin[:2] != company_gstin[:2]

#         # ---------- 6. Items XML ----------
#         items_xml = ""
#         for item in cn.items:
#             stock_group = item.item_group or "Primary"

#             qty = float(item.qty or 0)
#             # same hard-coded per_box as Sales Invoice; if needed, replace with item-specific factor
#             qty_str = qty_display(qty, item.uom, per_box=6)

#             cgst_rate = int(round(getattr(item, "cgst_rate", 0) or 0))
#             sgst_rate = int(round(getattr(item, "sgst_rate", 0) or 0))
#             igst_rate = int(round(getattr(item, "igst_rate", 0) or 0))

#             items_xml += f"""
#       <ALLINVENTORYENTRIES.LIST>
#        <STOCKITEMNAME>{escape_xml(item.item_name)}</STOCKITEMNAME>
#        <GSTOVRDNCLASSIFICATION>{escape_xml(stock_group)}</GSTOVRDNCLASSIFICATION>
#        <GSTOVRDNINELIGIBLEITC> Not Applicable</GSTOVRDNINELIGIBLEITC>
#        <GSTOVRDNISREVCHARGEAPPL> Not Applicable</GSTOVRDNISREVCHARGEAPPL>
#        <GSTOVRDNTAXABILITY>Taxable</GSTOVRDNTAXABILITY>
#        <GSTSOURCETYPE>Stock Group</GSTSOURCETYPE>
#        <HSNSOURCETYPE>Stock Group</HSNSOURCETYPE>
#        <GSTOVRDNTYPEOFSUPPLY>Goods</GSTOVRDNTYPEOFSUPPLY>
#        <GSTRATEINFERAPPLICABILITY>Use GST Classification</GSTRATEINFERAPPLICABILITY>
#        <GSTHSNINFERAPPLICABILITY>Use GST Classification</GSTHSNINFERAPPLICABILITY>
#        <HSNOVRDNCLASSIFICATION>{escape_xml(stock_group)}</HSNOVRDNCLASSIFICATION>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISGSTASSESSABLEVALUEOVERRIDDEN>No</ISGSTASSESSABLEVALUEOVERRIDDEN>
#        <RATE>{item.rate}/{item.uom}</RATE>
#        <AMOUNT>{-1 * float(item.amount or 0):.2f}</AMOUNT>
#        <ACTUALQTY>{qty_str}</ACTUALQTY>
#        <BILLEDQTY>{qty_str}</BILLEDQTY>
#        <BATCHALLOCATIONS.LIST>
#         <GODOWNNAME>Main Location</GODOWNNAME>
#         <BATCHNAME>Primary Batch</BATCHNAME>
#         <AMOUNT>{-1 * float(item.amount or 0):.2f}</AMOUNT>
#         <ACTUALQTY>{qty_str}</ACTUALQTY>
#         <BILLEDQTY>{qty_str}</BILLEDQTY>
#        </BATCHALLOCATIONS.LIST>
#        <ACCOUNTINGALLOCATIONS.LIST>
#         <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
#         <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#         <AMOUNT>{-1 * float(item.amount or 0):.2f}</AMOUNT>
#        </ACCOUNTINGALLOCATIONS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>CGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {cgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>SGST/UTGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {sgst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#        <RATEDETAILS.LIST>
#         <GSTRATEDUTYHEAD>IGST</GSTRATEDUTYHEAD>
#         <GSTRATEVALUATIONTYPE>Based on Value</GSTRATEVALUATIONTYPE>
#         <GSTRATE> {igst_rate:.2f}</GSTRATE>
#        </RATEDETAILS.LIST>
#       </ALLINVENTORYENTRIES.LIST>"""

#         # Party amount (credit note = positive for party)
#         party_amount = grand_total

#         # ---------- 7. Build XML body ----------
#         xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#  <HEADER>
#   <TALLYREQUEST>Import Data</TALLYREQUEST>
#  </HEADER>
#  <BODY>
#   <IMPORTDATA>
#    <REQUESTDESC>
#     <REPORTNAME>Vouchers</REPORTNAME>
#     <STATICVARIABLES>
#      <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
#     </STATICVARIABLES>
#    </REQUESTDESC>
#    <REQUESTDATA>
#     <TALLYMESSAGE xmlns:UDF="TallyUDF">
#      <VOUCHER REMOTEID="" VCHKEY="" VCHTYPE="Credit Note" ACTION="Create" OBJVIEW="Invoice Voucher View">
#       <DATE>{cn_date}</DATE>
#       <ISINVOICE>Yes</ISINVOICE>
#       <STATENAME>{escape_xml(state_name)}</STATENAME>
#       <COUNTRYOFRESIDENCE>India</COUNTRYOFRESIDENCE>
#       <PARTYGSTIN>{escape_xml(customer_gstin)}</PARTYGSTIN>
#       <PLACEOFSUPPLY>{escape_xml(state_name)}</PLACEOFSUPPLY>
#       <PARTYNAME>{escape_xml(customer_name)}</PARTYNAME>
#       <VOUCHERTYPENAME>Credit Note</VOUCHERTYPENAME>
#       <VOUCHERNUMBER>{escape_xml(cn.name)}</VOUCHERNUMBER>
#       <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
#       <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
#       <VCHGSTSTATUSISAPPLICABLE>Yes</VCHGSTSTATUSISAPPLICABLE>
#       {items_xml}
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>No</ISLASTDEEMEDPOSITIVE>
#        <AMOUNT>{-1 * party_amount:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         # Tax ledgers (reversed sign vs invoice)
#         if not interstate:
#             if total_cgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{-1 * total_cgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * total_cgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#             if total_sgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{-1 * total_sgst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * total_sgst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""
#         else:
#             if total_igst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>{-1 * total_igst:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * total_igst:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         if abs(roundoff) >= 0.01:
#             roundoff_sign = "Yes" if roundoff > 0 else "No"
#             xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
#        <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>{roundoff_sign}</ISDEEMEDPOSITIVE>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <ISLASTDEEMEDPOSITIVE>{roundoff_sign}</ISLASTDEEMEDPOSITIVE>
#        <ROUNDLIMIT> 1</ROUNDLIMIT>
#        <AMOUNT>{-1 * roundoff:.2f}</AMOUNT>
#        <VATEXPAMOUNT>{-1 * roundoff:.2f}</VATEXPAMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         xml_body += """
#      </VOUCHER>
#     </TALLYMESSAGE>
#    </REQUESTDATA>
#   </IMPORTDATA>
#  </BODY>
# </ENVELOPE>"""

#         # ---------- 8. Log and Send ----------
#         log = create_sync_log(
#             operation_type="Create Credit Note",
#             doctype_name="Sales Invoice",
#             doc_name=credit_note_name,
#             company=cn.company,
#             xml=xml_body,
#         )
#         result = send_xml_to_tally(log, xml_body)

#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Credit Note",
#                     document_name=credit_note_name,
#                     operation="Create Credit Note",
#                     error_message=result.get("error"),
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None,
#                 }
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name,
#             }

#         # ---------- 9. Extract voucher number ----------
#         voucher_number = cn.name
#         response_text = result.get("response", "") or ""
#         vch_start = response_text.find("<VOUCHERNUMBER>")
#         if vch_start != -1:
#             vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
#             if vch_end != -1:
#                 voucher_number = response_text[vch_start + 15 : vch_end].strip()

#         # ---------- 10. Update ERPNext doc ----------
#         try:
#             cn.db_set("custom_posted_to_tally", 1, update_modified=False)
#             cn.db_set("custom_tally_voucher_number", voucher_number, update_modified=False)
#             cn.db_set("custom_tally_push_status", "Success", update_modified=False)
#             cn.db_set("custom_tally_sync_date", now(), update_modified=False)
#             frappe.db.commit()
#         except Exception:
#             pass

#         return {
#             "success": True,
#             "message": f"Credit Note '{cn.name}' created in Tally",
#             "voucher_number": voucher_number,
#             "sync_log": log.name,
#         }

#     except Exception as e:
#         error_msg = f"Exception creating credit note: {str(e)}"
#         frappe.log_error("Tally Credit Note Creator", error_msg[:1000])
#         retry_job = create_retry_job(
#             document_type="Credit Note",
#             document_name=credit_note_name,
#             operation="Create Credit Note",
#             error_message=error_msg,
#         )
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None,
#         }

# API Method: tally.pushcreditnote
# Input: invoicename (Sales Invoice name which is a Credit Note: is_return = 1)
#        tallyurl (optional, falls back to Tally Settings or default)
# Output: frappe.response["message"] = {
#   "success": 1/0,
#   "vouchernumber": <CN name>,
#   "error": <msg>,
#   "logid": <Tally Sync Log name>
# }
import frappe
from datetime import datetime

import frappe

def get_reference_date_for_credit_note(credit_note_name: str) -> str | None:
    """Return reference date (sales invoice date) for given Credit Note."""
    # Load the Credit Note
    cn = frappe.get_doc("Sales Invoice", credit_note_name)

    if not cn.return_against:
        return None  # No linked sales invoice

    # Load the original Sales Invoice
    si = frappe.get_doc("Sales Invoice", cn.return_against)

    # Option 1: business date used on the invoice
    return str(si.posting_date)

    # Option 2 (if you really want DB creation timestamp instead of posting_date):
    # return str(si.creation)

# ============================================================================
# CLEAN CREDIT NOTE API (matches Sales Invoice pattern)
# ============================================================================

# @frappe.whitelist()
# def create_clean_credit_note_in_tally(credit_note_name):
#     """
#     Create Credit Note in Tally using clean XML builder.
#     Matches Sales Invoice master creation and validation pattern.
#     """
#     from tally_connect.tally_integration.api.validators import (
#         create_missing_masters_for_document,
#     )
    

#     try:
#         # ---------- 1. Load Credit Note ----------
#         cn = frappe.get_doc("Sales Invoice", credit_note_name)

#         if cn.docstatus != 1:
#             return {
#                 "success": False,
#                 "error": "Credit Note must be submitted before syncing to Tally",
#             }

#         if not cn.is_return:
#             return {
#                 "success": False,
#                 "error": "This is not a Credit Note (is_return is not set)",
#             }

#         # ---------- 1.a Settings and Tally company ----------
#         settings = get_settings()
#         if not settings.enabled:
#             return {
#                 "success": False,
#                 "error": "Tally integration is disabled in settings",
#             }

#         tally_company = get_tally_company_for_erpnext_company(cn.company)
#         if not tally_company:
#             tally_company = settings.tally_company_name

#         if not tally_company:
#             return {
#                 "success": False,
#                 "error": "No Tally company mapped for this ERPNext company",
#             }

#         # ---------- 2. Ensure all masters exist ----------
#         master_result = create_missing_masters_for_document("Sales Invoice", credit_note_name)

#         if not master_result.get("success"):
#             error_msg = "Could not create required masters in Tally: " + "; ".join(
#                 master_result.get("errors") or []
#             )
#             retry_job = create_retry_job(
#                 document_type="Credit Note",
#                 document_name=credit_note_name,
#                 operation="Create Credit Note",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#                 "created_masters": master_result.get("created", []),
#             }

#         # ---------- 3. Validate/Create Customer Ledger (safety net) ----------
#         customer_name = cn.customer_name or cn.customer
#         customer_check = check_master_exists("Ledger", customer_name)
#         if not customer_check.get("exists"):
#             customer_result = create_customer_ledger_in_tally(cn.customer, cn.company)
#             if not customer_result.get("success"):
#                 return {
#                     "success": False,
#                     "error": (
#                         f"Customer ledger '{customer_name}' does not exist and "
#                         f"auto-creation failed: {customer_result.get('error')}"
#                     ),
#                     "retry_job": customer_result.get("retry_job"),
#                 }

#         # ---------- 4. Resolve Ledgers (now assumed to exist) ----------
#         required_ledgers = {}

#         sales_ledger = settings.sales_ledger_name or "SALES A/C"
#         required_ledgers["Sales"] = sales_ledger

#         cgst_ledger = settings.cgst_ledger_name or "CGST"
#         sgst_ledger = settings.sgst_ledger_name or "SGST"
#         igst_ledger = settings.igst_ledger_name or "IGST"
#         required_ledgers["CGST"] = cgst_ledger
#         required_ledgers["SGST"] = sgst_ledger
#         required_ledgers["IGST"] = igst_ledger

#         round_off_ledger = settings.round_off_ledger_name or "Round Off"
#         required_ledgers["Round Off"] = round_off_ledger
        
#         # Optional sanity check: if any still missing, fail early
#         missing_ledgers = []
#         for ledger_type, ledger_name in required_ledgers.items():
#             ledger_check = check_master_exists("Ledger", ledger_name)
#             if not ledger_check.get("exists"):
#                 missing_ledgers.append(f"{ledger_type} ({ledger_name})")

#         if missing_ledgers:
#             error_msg = f"Missing ledgers in Tally even after master creation: {', '.join(missing_ledgers)}"
#             retry_job = create_retry_job(
#                 document_type="Credit Note",
#                 document_name=credit_note_name,
#                 operation="Create Credit Note",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#             }

#         # ---------- 5. Validate Stock Items (safety check) ----------
#         missing_items = []
#         for item in cn.items:
#             item_check = check_master_exists("StockItem", item.item_name)
#             if not item_check.get("exists"):
#                 missing_items.append(item.item_name)

#         if missing_items:
#             error_msg = f"Missing stock items in Tally: {', '.join(missing_items[:5])}"
#             if len(missing_items) > 5:
#                 error_msg += f" and {len(missing_items) - 5} more"

#             retry_job = create_retry_job(
#                 document_type="Credit Note",
#                 document_name=credit_note_name,
#                 operation="Create Credit Note",
#                 error_message=error_msg,
#             )
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "retry_job": retry_job.name if retry_job else None,
#             }

#         # ---------- 6. Build Voucher XML ----------

#         # def qty_display(qty, uom, per_box=6):
#         #     """Display quantity: ' 150 Pcs = 25 Box' (hardcoded per_box=6)"""
#         #     base = abs(float(qty))
#         #     boxes = int(base // per_box)
#         #     if base == int(base):
#         #         base_str = f"{int(base)}"
#         #     else:
#         #         base_str = f"{base:.2f}"
#         #     if uom:
#         #         return f" {base_str} {uom} = {boxes} Box"
#         #     return f" {base_str} = {boxes} Box"

#         def to_yyyymmdd(val):
#             return val.strftime("%Y%m%d") if isinstance(val, datetime) else str(val)

#         def to_dd_mmm_yyyy(val):
#             return val.strftime("%d-%b-%Y") if isinstance(val, datetime) else str(val)

#         # Core values
#         cn_date = to_yyyymmdd(cn.posting_date)
#         effective_date = cn_date
#         ref_date = get_reference_date_for_credit_note(cn.name)
#         if ref_date:
#             ref_date_yyyymmdd = ref_date.replace("-", "")


#         place_of_supply = cn.place_of_supply
#         if "-" in place_of_supply:
#             state_name = place_of_supply.split("-")[1].strip()
#         else:
#             state_name = place_of_supply

#         # Original invoice details
#         original_inv = cn.return_against or ""
#         original_inv_date = cn.posting_date
#         original_date_text = ""
#         if original_inv:
#             try:
#                 orig_doc = frappe.get_doc("Sales Invoice", original_inv)
#                 original_inv_date = orig_doc.posting_date
#                 original_date_text = to_dd_mmm_yyyy(original_inv_date)
#             except Exception:
#                 original_date_text = to_dd_mmm_yyyy(cn.posting_date)
#         else:
#             original_date_text = to_dd_mmm_yyyy(cn.posting_date)

#         # Tax totals
#         total_igst = total_cgst = total_sgst = 0.0
#         for tax_line in cn.taxes or []:
#             gst_type = (tax_line.gst_tax_type or "").lower()
#             tax_amount = abs(float(tax_line.tax_amount or 0))
#             if gst_type == "igst":
#                 total_igst += tax_amount
#             elif gst_type == "cgst":
#                 total_cgst += tax_amount
#             elif gst_type == "sgst":
#                 total_sgst += tax_amount

#         total_igst = round(total_igst, 2)
#         total_cgst = round(total_cgst, 2)
#         total_sgst = round(total_sgst, 2)

#         grand_total = abs(round(float(cn.base_rounded_total or cn.grand_total or 0), 2))
#         roundoff = float(cn.rounding_adjustment or 0)
#         party_amount = grand_total

#         # Inter vs intra
#         interstate = False
#         company_gstin = cn.company_gstin or ""
#         customer_gstin = cn.billing_address_gstin or cn.customer_gstin or ""
#         if customer_gstin and company_gstin:
#             interstate = customer_gstin[:2] != company_gstin[:2]

#         # Items XML
#         items_xml = ""
#         for item in cn.items:
#             if not item.qty:
#                 continue

#             # Load Item doc to access UOM conversions
#             item_doc = frappe.get_doc("Item", item.item_code)

#             stock_group = item.item_group or "Primary"

#             # New dynamic qty display (no per_box hardcoding)
#             qty_str = qty_display_for_item(item, item_doc)

#             line_amount = abs(float(item.base_amount or item.amount or 0))
#             rate = abs(float(item.base_rate or item.rate or 0))
#             rate_str = f"{rate}/{item.uom}" if item.uom else f"{rate}"

#             items_xml += f"""
#             <ALLINVENTORYENTRIES.LIST>
#             <STOCKITEMNAME>{escape_xml(item.item_name or item.item_code)}</STOCKITEMNAME>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <RATE>{rate_str}</RATE>
#             <AMOUNT>-{line_amount:.2f}</AMOUNT>
#             <ACTUALQTY>{qty_str}</ACTUALQTY>
#             <BILLEDQTY>{qty_str}</BILLEDQTY>
#             <BATCHALLOCATIONS.LIST>
#             <GODOWNNAME>Main Location</GODOWNNAME>
#             <BATCHNAME>Primary Batch</BATCHNAME>
#             <AMOUNT>-{line_amount:.2f}</AMOUNT>
#             <ACTUALQTY>{qty_str}</ACTUALQTY>
#             <BILLEDQTY>{qty_str}</BILLEDQTY>
#             </BATCHALLOCATIONS.LIST>
#             <ACCOUNTINGALLOCATIONS.LIST>
#             <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <LEDGERFROMITEM>No</LEDGERFROMITEM>
#             <ISPARTYLEDGER>No</ISPARTYLEDGER>
#             <AMOUNT>-{line_amount:.2f}</AMOUNT>
#             </ACCOUNTINGALLOCATIONS.LIST>
#             </ALLINVENTORYENTRIES.LIST>"""

#         def split_lines(val):
#             return [l.strip() for l in (val or "").split("\n") if l.strip()]

#         # Buyer (bill‑to) from Sales Invoice.address_display
#         buyer_lines = split_lines(getattr(cn, "address_display", ""))

#         buyer_xml = ""
#         if buyer_lines:
#             buyer_xml = (
#                 '\n      <BASICBUYERADDRESS.LIST TYPE="String">'
#                 + "".join(
#                     f"\n       <BASICBUYERADDRESS>{escape_xml(l)}</BASICBUYERADDRESS>"
#                     for l in buyer_lines
#                 )
#                 + "\n      </BASICBUYERADDRESS.LIST>"
#             )

#         # Build XML body
#         xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
# <ENVELOPE>
#  <HEADER>
#   <TALLYREQUEST>Import Data</TALLYREQUEST>
#  </HEADER>
#  <BODY>
#   <IMPORTDATA>
#    <REQUESTDESC>
#     <REPORTNAME>Vouchers</REPORTNAME>
#     <STATICVARIABLES>
#      <SVCURRENTCOMPANY>{escape_xml(tally_company)}</SVCURRENTCOMPANY>
#     </STATICVARIABLES>
#    </REQUESTDESC>
#    <REQUESTDATA>
#     <TALLYMESSAGE xmlns:UDF="TallyUDF">
#      <VOUCHER VCHTYPE="CREDIT NOTE" ACTION="Create" OBJVIEW="Invoice Voucher View">
#      <ADDRESS.LIST TYPE="String">
#        <ADDRESS>TEST</ADDRESS>
#       </ADDRESS.LIST>
#       <STATENAME>Buyer Address</STATENAME>
#       <COUNTRYOFRESIDENCE>Buyer Country</COUNTRYOFRESIDENCE>
#       <PARTYGSTIN>Buyer GSTIN</PARTYGSTIN>
#       <PLACEOFSUPPLY>Maharashtra</PLACEOFSUPPLY>
#       <BASICBUYERADDRESS.LIST TYPE="String">
#        <BASICBUYERADDRESS>TEST</BASICBUYERADDRESS>
#       </BASICBUYERADDRESS.LIST>
#       <DATE>{cn_date}</DATE>
#       <VCHSTATUSDATE>{cn_date}</VCHSTATUSDATE>
#       <REFERENCEDATE>{ref_date_yyyymmdd}</REFERENCEDATE>
#       <NARRATION>{escape_xml(cn.remarks or (f"Credit note issued against {original_inv}" if original_inv else "Sales return"))}</NARRATION>
#       <CMPGSTIN>{escape_xml(company_gstin)}</CMPGSTIN>
#       <VOUCHERTYPENAME>CREDIT NOTE</VOUCHERTYPENAME>
#       <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
#       <VOUCHERNUMBER>{escape_xml(cn.name)}</VOUCHERNUMBER>
#       <REFERENCE>{escape_xml(cn.return_against)}</REFERENCE>
      

#       <INVOICEORDERLIST.LIST>
#         <BASICPURCHASEORDERNO>{cn.po_no}</BASICPURCHASEORDERNO>
#        <BASICORDERDATE>{cn.po_date}</BASICORDERDATE>
       
#       </INVOICEORDERLIST.LIST>
      

#       <CMPGSTREGISTRATIONTYPE>Regular</CMPGSTREGISTRATIONTYPE>
#       <CMPGSTSTATE>{escape_xml(state_name)}</CMPGSTSTATE>
#       <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
#       <BASICORDERREF>Against Invoice: {escape_xml(original_inv or cn.name)} dated {original_date_text}</BASICORDERREF>
#       <EFFECTIVEDATE>{effective_date}</EFFECTIVEDATE>
#       <ISINVOICE>Yes</ISINVOICE>

# {items_xml}

#       <!-- Party ledger -->
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
#        <LEDGERFROMITEM>No</LEDGERFROMITEM>
#        <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
#        <AMOUNT>{party_amount:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         # Tax ledgers
#         if not interstate:
#             if total_cgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <LEDGERFROMITEM>No</LEDGERFROMITEM>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>-{total_cgst:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""
#             if total_sgst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <LEDGERFROMITEM>No</LEDGERFROMITEM>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>-{total_sgst:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""
#         else:
#             if total_igst > 0:
#                 xml_body += f"""
#       <LEDGERENTRIES.LIST>
#        <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
#        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#        <LEDGERFROMITEM>No</LEDGERFROMITEM>
#        <ISPARTYLEDGER>No</ISPARTYLEDGER>
#        <AMOUNT>-{total_igst:.2f}</AMOUNT>
#       </LEDGERENTRIES.LIST>"""

#         # Round off
#         # Round off - CORRECT logic for both + and - values
#         # Read the value from ERPNext
#         roundoff = float(cn.rounding_adjustment or 0)

#         # For Tally XML, just negate it
#         if abs(roundoff) >= 0.01:
#             xml_body += f"""
#             <LEDGERENTRIES.LIST>
#             <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
#             <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
#             <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
#             <ISPARTYLEDGER>No</ISPARTYLEDGER>
#             <ISLASTDEEMEDPOSITIVE>Yes</ISLASTDEEMEDPOSITIVE>
#             <ROUNDLIMIT> 1</ROUNDLIMIT>
#             <AMOUNT>{roundoff:.2f}</AMOUNT>
#             <VATEXPAMOUNT>{roundoff:.2f}</VATEXPAMOUNT>
#             </LEDGERENTRIES.LIST>"""

#         # BEFORE the final xml_body += "</VOUCHER>..."
#         ship_lines = split_lines(getattr(cn, "shipping_address", ""))
#         consignee_xml = ""
#         if ship_lines:
#             consignee_xml = (
#                 '\n       <CONSIGNEEADDRESS.LIST TYPE="String">'
#                 + "".join(
#                     f"\n        <CONSIGNEEADDRESS>{escape_xml(l)}</CONSIGNEEADDRESS>"
#                     for l in ship_lines
#                 )
#                 + "\n       </CONSIGNEEADDRESS.LIST>"
#             )

#         consignor_full = escape_xml(cn.company)  # or full company address string
#         consignor_place = escape_xml(place_of_supply)  # e.g. THANE
#         consignor_pincode = ""      # pull from company address doc if needed
#         consignee_place = escape_xml(place_of_supply)  # e.g. HARYANA / city
#         consignee_pincode = ""      # from shipping address
#         from_state = escape_xml(state_name)
#         to_state = escape_xml(state_name)

#         # AFTER adding round-off LEDGERENTRIES, BEFORE closing the voucher
#         if consignee_xml:
#             xml_body += f"""
#             <EWAYBILLDETAILS.LIST>
#             <CONSIGNORADDRESS.LIST TYPE="String">
#                 <CONSIGNORADDRESS>{consignor_full}</CONSIGNORADDRESS>
#             </CONSIGNORADDRESS.LIST>{consignee_xml}
#             <DOCUMENTTYPE>Others</DOCUMENTTYPE>
#             <CONSIGNEEPINCODE>{consignee_pincode}</CONSIGNEEPINCODE>
#             <SUBTYPE>Sales Return</SUBTYPE>
#             <CONSIGNORPLACE>{consignor_place}</CONSIGNORPLACE>
#             <CONSIGNORPINCODE>{consignor_pincode}</CONSIGNORPINCODE>
#             <CONSIGNEEPLACE>{consignee_place}</CONSIGNEEPLACE>
#             <SHIPPEDFROMSTATE>{from_state}</SHIPPEDFROMSTATE>
#             <SHIPPEDTOSTATE>{to_state}</SHIPPEDTOSTATE>
#             </EWAYBILLDETAILS.LIST>"""





#         xml_body += """
#      </VOUCHER>
#     </TALLYMESSAGE>
#    </REQUESTDATA>
#   </IMPORTDATA>
#  </BODY>
# </ENVELOPE>"""

#         # ---------- 7. Log and Send ----------
#         log = create_sync_log(
#             operation_type="Create Credit Note",
#             doctype_name="Sales Invoice",
#             doc_name=credit_note_name,
#             company=cn.company,
#             xml=xml_body,
#         )
#         result = send_xml_to_tally(log, xml_body)

#         if not result.get("success"):
#             if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
#                 retry_job = create_retry_job(
#                     document_type="Credit Note",
#                     document_name=credit_note_name,
#                     operation="Create Credit Note",
#                     error_message=result.get("error"),
#                 )
#                 return {
#                     "success": False,
#                     "error": result.get("error"),
#                     "sync_log": log.name,
#                     "retry_job": retry_job.name if retry_job else None,
#                 }
#             return {
#                 "success": False,
#                 "error": result.get("error"),
#                 "sync_log": log.name,
#             }

#         # ---------- 8. Extract voucher number ----------
#         voucher_number = cn.name
#         response_text = result.get("response", "") or ""
#         vch_start = response_text.find("<VOUCHERNUMBER>")
#         if vch_start != -1:
#             vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
#             if vch_end != -1:
#                 voucher_number = response_text[vch_start + 15: vch_end].strip()

#         # ---------- 9. Update ERPNext doc ----------
#         try:
#             cn.db_set("custom_posted_to_tally", 1, update_modified=False)
#             cn.db_set("custom_tally_voucher_number", voucher_number, update_modified=False)
#             cn.db_set("custom_tally_push_status", "Success", update_modified=False)
#             cn.db_set("custom_tally_sync_date", frappe.utils.now(), update_modified=False)
#             frappe.db.commit()
#         except Exception:
#             pass

#         return {
#             "success": True,
#             "message": f"Credit Note '{cn.name}' created in Tally",
#             "voucher_number": voucher_number,
#             "sync_log": log.name,
#         }

#     except Exception as e:
#         error_msg = f"Exception creating credit note: {str(e)}"
#         frappe.log_error("Tally Credit Note Creator", error_msg[:1000])
#         retry_job = create_retry_job(
#             document_type="Credit Note",
#             document_name=credit_note_name,
#             operation="Create Credit Note",
#             error_message=error_msg,
#         )
#         return {
#             "success": False,
#             "error": error_msg,
#             "retry_job": retry_job.name if retry_job else None,
#         }
import frappe
from datetime import datetime

def address_two_lines(addr):
            """Return [line1, line2] built from Address doc."""
            if not addr:
                return []
            line1 = addr.address_line1 or ""
            bits = []
            if addr.address_line2:
                bits.append(addr.address_line2)
            if addr.city:
                bits.append(addr.city)
            if addr.state:
                bits.append(addr.state)
            if addr.pincode:
                bits.append(addr.pincode)
            line2 = ", ".join(bits) if bits else ""
            return [l for l in (line1, line2) if l]

def get_reference_date_for_credit_note(credit_note_name: str) -> str | None:
    cn = frappe.get_doc("Sales Invoice", credit_note_name)
    if not cn.return_against:
        return None
    si = frappe.get_doc("Sales Invoice", cn.return_against)
    return str(si.posting_date)


@frappe.whitelist()
def create_clean_credit_note_in_tally(credit_note_name):
    """
    Create Credit Note in Tally using clean XML builder.
    Matches Sales Invoice master creation and validation pattern.
    """
    from tally_connect.tally_integration.api.validators import (
        create_missing_masters_for_document,
    )

    try:
        # ---------- 1. Load Credit Note ----------
        cn = frappe.get_doc("Sales Invoice", credit_note_name)

        if cn.docstatus != 1:
            return {
                "success": False,
                "error": "Credit Note must be submitted before syncing to Tally",
            }

        if not cn.is_return:
            return {
                "success": False,
                "error": "This is not a Credit Note (is_return is not set)",
            }

        # ---------- 1.a Settings and Tally company ----------
        settings = get_settings()
        if not settings.enabled:
            return {
                "success": False,
                "error": "Tally integration is disabled in settings",
            }

        tally_company = get_tally_company_for_erpnext_company(cn.company)
        if not tally_company:
            tally_company = settings.tally_company_name

        if not tally_company:
            return {
                "success": False,
                "error": "No Tally company mapped for this ERPNext company",
            }

        # ---------- 2. Ensure all masters exist ----------
        master_result = create_missing_masters_for_document("Sales Invoice", credit_note_name)

        if not master_result.get("success"):
            error_msg = "Could not create required masters in Tally: " + "; ".join(
                master_result.get("errors") or []
            )
            retry_job = create_retry_job(
                document_type="Credit Note",
                document_name=credit_note_name,
                operation="Create Credit Note",
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
                "created_masters": master_result.get("created", []),
            }

        # ---------- 3. Validate/Create Customer Ledger ----------
        customer_name = cn.customer_name or cn.customer
        customer_check = check_master_exists("Ledger", customer_name)
        if not customer_check.get("exists"):
            customer_result = create_customer_ledger_in_tally(cn.customer, cn.company)
            if not customer_result.get("success"):
                return {
                    "success": False,
                    "error": (
                        f"Customer ledger '{customer_name}' does not exist and "
                        f"auto-creation failed: {customer_result.get('error')}"
                    ),
                    "retry_job": customer_result.get("retry_job"),
                }

        # ---------- 4. Resolve Ledgers ----------
        required_ledgers = {}

        sales_ledger = settings.sales_ledger_name or "SALES A/C"
        required_ledgers["Sales"] = sales_ledger

        cgst_ledger = settings.cgst_ledger_name or "CGST"
        sgst_ledger = settings.sgst_ledger_name or "SGST"
        igst_ledger = settings.igst_ledger_name or "IGST"
        required_ledgers["CGST"] = cgst_ledger
        required_ledgers["SGST"] = sgst_ledger
        required_ledgers["IGST"] = igst_ledger

        round_off_ledger = settings.round_off_ledger_name or "Round Off"
        required_ledgers["Round Off"] = round_off_ledger

        missing_ledgers = []
        for ledger_type, ledger_name in required_ledgers.items():
            ledger_check = check_master_exists("Ledger", ledger_name)
            if not ledger_check.get("exists"):
                missing_ledgers.append(f"{ledger_type} ({ledger_name})")

        if missing_ledgers:
            error_msg = (
                "Missing ledgers in Tally even after master creation: "
                + ", ".join(missing_ledgers)
            )
            retry_job = create_retry_job(
                document_type="Credit Note",
                document_name=credit_note_name,
                operation="Create Credit Note",
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
            }

        # ---------- 5. Validate Stock Items ----------
        missing_items = []
        for item in cn.items:
            item_check = check_master_exists("StockItem", item.item_name)
            if not item_check.get("exists"):
                missing_items.append(item.item_name)

        if missing_items:
            error_msg = f"Missing stock items in Tally: {', '.join(missing_items[:5])}"
            if len(missing_items) > 5:
                error_msg += f" and {len(missing_items) - 5} more"

            retry_job = create_retry_job(
                document_type="Credit Note",
                document_name=credit_note_name,
                operation="Create Credit Note",
                error_message=error_msg,
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None,
            }

        # ---------- 6. Build Voucher XML ----------

        def to_yyyymmdd(val):
            return val.strftime("%Y%m%d") if isinstance(val, datetime) else str(val)

        def to_dd_mmm_yyyy(val):
            return val.strftime("%d-%b-%Y") if isinstance(val, datetime) else str(val)

        def address_two_lines(addr):
            """Return [line1, line2] built from Address doc."""
            if not addr:
                return []
            line1 = addr.address_line1 or ""
            bits = []
            if addr.address_line2:
                bits.append(addr.address_line2)
            if addr.city:
                bits.append(addr.city)
            if addr.state:
                bits.append(addr.state)
            if addr.pincode:
                bits.append(addr.pincode)
            line2 = ", ".join(bits) if bits else ""
            return [l for l in (line1, line2) if l]

        # Core values
        cn_date = to_yyyymmdd(cn.posting_date)
        effective_date = cn_date
        ref_date = get_reference_date_for_credit_note(cn.name)
        ref_date_yyyymmdd = ref_date.replace("-", "") if ref_date else ""

        place_of_supply = cn.place_of_supply
        if "-" in place_of_supply:
            state_name = place_of_supply.split("-")[1].strip()
        else:
            state_name = place_of_supply

        # Original invoice details
        original_inv = cn.return_against or ""
        original_inv_date = cn.posting_date
        original_date_text = ""
        if original_inv:
            try:
                orig_doc = frappe.get_doc("Sales Invoice", original_inv)
                original_inv_date = orig_doc.posting_date
                original_date_text = to_dd_mmm_yyyy(original_inv_date)
            except Exception:
                original_date_text = to_dd_mmm_yyyy(cn.posting_date)
        else:
            original_date_text = to_dd_mmm_yyyy(cn.posting_date)

        # Tax totals
        total_igst = total_cgst = total_sgst = 0.0
        for tax_line in cn.taxes or []:
            gst_type = (tax_line.gst_tax_type or "").lower()
            tax_amount = abs(float(tax_line.tax_amount or 0))
            if gst_type == "igst":
                total_igst += tax_amount
            elif gst_type == "cgst":
                total_cgst += tax_amount
            elif gst_type == "sgst":
                total_sgst += tax_amount

        total_igst = round(total_igst, 2)
        total_cgst = round(total_cgst, 2)
        total_sgst = round(total_sgst, 2)

        grand_total = abs(round(float(cn.base_rounded_total or cn.grand_total or 0), 2))
        roundoff = float(cn.rounding_adjustment or 0)
        party_amount = grand_total

        # Inter vs intra
        interstate = False
        company_gstin = cn.company_gstin or ""
        customer_gstin = cn.billing_address_gstin or cn.customer_gstin or ""
        if customer_gstin and company_gstin:
            interstate = customer_gstin[:2] != company_gstin[:2]

        # ---------- 6.a Addresses from Address doctypes ----------
        billing_addr = frappe.get_doc("Address", cn.customer_address) if cn.customer_address else None
        shipping_addr = (
            frappe.get_doc("Address", cn.shipping_address_name)
            if getattr(cn, "shipping_address_name", None)
            else None
        )

        buyer_lines = address_two_lines(billing_addr)
        ship_lines = address_two_lines(shipping_addr)

        buyer_xml = ""
        if buyer_lines:
            buyer_xml = (
                '\n      <BASICBUYERADDRESS.LIST TYPE="String">'
                + "".join(
                    f"\n       <BASICBUYERADDRESS>{escape_xml(l)}</BASICBUYERADDRESS>"
                    for l in buyer_lines
                )
                + "\n      </BASICBUYERADDRESS.LIST>"
            )

        consignee_xml = ""
        if ship_lines:
            consignee_xml = (
                '\n       <CONSIGNEEADDRESS.LIST TYPE="String">'
                + "".join(
                    f"\n        <CONSIGNEEADDRESS>{escape_xml(l)}</CONSIGNEEADDRESS>"
                    for l in ship_lines
                )
                + "\n       </CONSIGNEEADDRESS.LIST>"
            )

        buyer_state = escape_xml(billing_addr.state) if billing_addr and billing_addr.state else ""
        buyer_country = escape_xml(billing_addr.country) if billing_addr and billing_addr.country else ""
        cons_state = escape_xml(shipping_addr.state) if shipping_addr and shipping_addr.state else ""
        cons_country = escape_xml(shipping_addr.country) if shipping_addr and shipping_addr.country else ""
        cons_pincode = escape_xml(shipping_addr.pincode) if shipping_addr and shipping_addr.pincode else ""
        cons_city = escape_xml(shipping_addr.city) if shipping_addr and shipping_addr.city else ""
        bill_city = escape_xml(billing_addr.city) if billing_addr and billing_addr.city else ""
        bill_pincode = escape_xml(billing_addr.pincode) if billing_addr and billing_addr.pincode else ""

        # ---------- 6.b Items XML ----------
        items_xml = ""
        for item in cn.items:
            if not item.qty:
                continue

            item_doc = frappe.get_doc("Item", item.item_code)
            qty_str = qty_display_for_item(item, item_doc)

            line_amount = abs(float(item.base_amount or item.amount or 0))
            rate = abs(float(item.base_rate or item.rate or 0))
            rate_str = f"{rate}/{item.uom}" if item.uom else f"{rate}"

            items_xml += f"""
      <ALLINVENTORYENTRIES.LIST>
       <STOCKITEMNAME>{escape_xml(item.item_name or item.item_code)}</STOCKITEMNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <RATE>{rate_str}</RATE>
       <AMOUNT>-{line_amount:.2f}</AMOUNT>
       <ACTUALQTY>{qty_str}</ACTUALQTY>
       <BILLEDQTY>{qty_str}</BILLEDQTY>
       <BATCHALLOCATIONS.LIST>
        <GODOWNNAME>Main Location</GODOWNNAME>
        <BATCHNAME>Primary Batch</BATCHNAME>
        <AMOUNT>-{line_amount:.2f}</AMOUNT>
        <ACTUALQTY>{qty_str}</ACTUALQTY>
        <BILLEDQTY>{qty_str}</BILLEDQTY>
       </BATCHALLOCATIONS.LIST>
       <ACCOUNTINGALLOCATIONS.LIST>
        <LEDGERNAME>{escape_xml(sales_ledger)}</LEDGERNAME>
        <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
        <LEDGERFROMITEM>No</LEDGERFROMITEM>
        <ISPARTYLEDGER>No</ISPARTYLEDGER>
        <AMOUNT>-{line_amount:.2f}</AMOUNT>
       </ACCOUNTINGALLOCATIONS.LIST>
      </ALLINVENTORYENTRIES.LIST>"""

        # ---------- 6.c Build XML body ----------
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
     <VOUCHER VCHTYPE="CREDIT NOTE" ACTION="Create" OBJVIEW="Invoice Voucher View">
     <ADDRESS.LIST TYPE="String">
        <ADDRESS>{escape_xml(buyer_lines[0])}</ADDRESS>
        <ADDRESS>{escape_xml(buyer_lines[1])}</ADDRESS>
    </ADDRESS.LIST>

    
      <BASICBUYERADDRESS.LIST TYPE="String">

       <BASICBUYERADDRESS>{escape_xml(ship_lines[0])}</BASICBUYERADDRESS>
       <BASICBUYERADDRESS>{escape_xml(ship_lines[1])}</BASICBUYERADDRESS>
      </BASICBUYERADDRESS.LIST>
      
      <CONSIGNEESTATENAME>{cons_state}</CONSIGNEESTATENAME>
      <CONSIGNEECOUNTRYNAME>{cons_country}</CONSIGNEECOUNTRYNAME>    

     
      
     
      <DATE>{cn_date}</DATE>
      <VCHSTATUSDATE>{cn_date}</VCHSTATUSDATE>
      <PARTYPINCODE>{bill_pincode}</PARTYPINCODE>
      <CONSIGNEEPINCODE>{bill_pincode}</CONSIGNEEPINCODE>
      <REFERENCEDATE>{ref_date_yyyymmdd}</REFERENCEDATE>
      <STATENAME>{buyer_state}</STATENAME>
      <COUNTRYOFRESIDENCE>{buyer_country or 'India'}</COUNTRYOFRESIDENCE>
      <PARTYGSTIN>{escape_xml(customer_gstin)}</PARTYGSTIN>
      <PLACEOFSUPPLY>{escape_xml(place_of_supply)}</PLACEOFSUPPLY>
      <CONSIGNEESTATENAME>{cons_state}</CONSIGNEESTATENAME>
      <CONSIGNEECOUNTRYNAME>{cons_country or 'India'}</CONSIGNEECOUNTRYNAME>
      <NARRATION>{escape_xml(cn.remarks or (f"Credit note issued against {original_inv}" if original_inv else "Sales return"))}</NARRATION>
      <CMPGSTIN>{escape_xml(company_gstin)}</CMPGSTIN>
      <VOUCHERTYPENAME>CREDIT NOTE</VOUCHERTYPENAME>
      <PARTYLEDGERNAME>{escape_xml(customer_name)}</PARTYLEDGERNAME>
      <VOUCHERNUMBER>{escape_xml(cn.name)}</VOUCHERNUMBER>
      <REFERENCE>{escape_xml(cn.return_against)}</REFERENCE>
      <INVOICEORDERLIST.LIST>
       <BASICPURCHASEORDERNO>{escape_xml(cn.po_no or "")}</BASICPURCHASEORDERNO>
       <BASICORDERDATE>{escape_xml(str(cn.po_date) if cn.po_date else "")}</BASICORDERDATE>
      </INVOICEORDERLIST.LIST>
      <CMPGSTREGISTRATIONTYPE>Regular</CMPGSTREGISTRATIONTYPE>
      <CMPGSTSTATE>{escape_xml(state_name)}</CMPGSTSTATE>
      <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>
      <BASICORDERREF>Against Invoice: {escape_xml(original_inv or cn.name)} dated {original_date_text}</BASICORDERREF>
      <EFFECTIVEDATE>{effective_date}</EFFECTIVEDATE>
      <ISINVOICE>Yes</ISINVOICE>

{items_xml}

      <!-- Party ledger -->
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(customer_name)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
       <LEDGERFROMITEM>No</LEDGERFROMITEM>
       <ISPARTYLEDGER>Yes</ISPARTYLEDGER>
       <AMOUNT>{party_amount:.2f}</AMOUNT>
      </LEDGERENTRIES.LIST>"""

        # ---------- Tax ledgers ----------
        if not interstate:
            if total_cgst > 0:
                xml_body += f"""
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(cgst_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <LEDGERFROMITEM>No</LEDGERFROMITEM>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <AMOUNT>-{total_cgst:.2f}</AMOUNT>
      </LEDGERENTRIES.LIST>"""
            if total_sgst > 0:
                xml_body += f"""
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(sgst_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <LEDGERFROMITEM>No</LEDGERFROMITEM>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <AMOUNT>-{total_sgst:.2f}</AMOUNT>
      </LEDGERENTRIES.LIST>"""
        else:
            if total_igst > 0:
                xml_body += f"""
      <LEDGERENTRIES.LIST>
       <LEDGERNAME>{escape_xml(igst_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <LEDGERFROMITEM>No</LEDGERFROMITEM>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <AMOUNT>-{total_igst:.2f}</AMOUNT>
      </LEDGERENTRIES.LIST>"""

        # ---------- Round off ----------
        if abs(roundoff) >= 0.01:
            xml_body += f"""
      <LEDGERENTRIES.LIST>
       <ROUNDTYPE>Normal Rounding</ROUNDTYPE>
       <LEDGERNAME>{escape_xml(round_off_ledger)}</LEDGERNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <ISPARTYLEDGER>No</ISPARTYLEDGER>
       <ISLASTDEEMEDPOSITIVE>Yes</ISLASTDEEMEDPOSITIVE>
       <ROUNDLIMIT> 1</ROUNDLIMIT>
       <AMOUNT>{roundoff:.2f}</AMOUNT>
       <VATEXPAMOUNT>{roundoff:.2f}</VATEXPAMOUNT>
      </LEDGERENTRIES.LIST>"""

      # ---------- E-way / Consignee block ----------
        if consignee_xml:
            xml_body += f"""
      <EWAYBILLDETAILS.LIST>
       <CONSIGNORADDRESS.LIST TYPE="String">
        <CONSIGNORADDRESS>{escape_xml(tally_company)}</CONSIGNORADDRESS>
       </CONSIGNORADDRESS.LIST>
       {consignee_xml}

       <DOCUMENTTYPE>Others</DOCUMENTTYPE>
       <CONSIGNEEPINCODE>{cons_pincode}</CONSIGNEEPINCODE>
       <SUBTYPE>Sales Return</SUBTYPE>
       <CONSIGNORPLACE>{bill_city}</CONSIGNORPLACE>
       <CONSIGNORPINCODE>{bill_pincode}</CONSIGNORPINCODE>
       <CONSIGNEEPLACE>{cons_city}</CONSIGNEEPLACE>
       <SHIPPEDFROMSTATE>{buyer_state}</SHIPPEDFROMSTATE>
       <SHIPPEDTOSTATE>{cons_state}</SHIPPEDTOSTATE>
      </EWAYBILLDETAILS.LIST>"""

        

        xml_body += """
     </VOUCHER>
    </TALLYMESSAGE>
   </REQUESTDATA>
  </IMPORTDATA>
 </BODY>
</ENVELOPE>"""

        # ---------- 7. Log and Send ----------
        log = create_sync_log(
            operation_type="Create Credit Note",
            doctype_name="Sales Invoice",
            doc_name=credit_note_name,
            company=cn.company,
            xml=xml_body,
        )
        result = send_xml_to_tally(log, xml_body)

        if not result.get("success"):
            if result.get("error_type") in ["NETWORK ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Credit Note",
                    document_name=credit_note_name,
                    operation="Create Credit Note",
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

        # ---------- 8. Extract voucher number ----------
        voucher_number = cn.name
        response_text = result.get("response", "") or ""
        vch_start = response_text.find("<VOUCHERNUMBER>")
        if vch_start != -1:
            vch_end = response_text.find("</VOUCHERNUMBER>", vch_start)
            if vch_end != -1:
                voucher_number = response_text[vch_start + 15: vch_end].strip()

        # ---------- 9. Update ERPNext doc ----------
        try:
            cn.db_set("custom_posted_to_tally", 1, update_modified=False)
            cn.db_set("custom_tally_voucher_number", voucher_number, update_modified=False)
            cn.db_set("custom_tally_push_status", "Success", update_modified=False)
            cn.db_set("custom_tally_sync_date", frappe.utils.now(), update_modified=False)
            frappe.db.commit()
        except Exception:
            pass

        return {
            "success": True,
            "message": f"Credit Note '{cn.name}' created in Tally",
            "voucher_number": voucher_number,
            "sync_log": log.name,
        }

    except Exception as e:
        error_msg = f"Exception creating credit note: {str(e)}"
        frappe.log_error("Tally Credit Note Creator", error_msg)
        retry_job = create_retry_job(
            document_type="Credit Note",
            document_name=credit_note_name,
            operation="Create Credit Note",
            error_message=error_msg,
        )
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None,
        }
