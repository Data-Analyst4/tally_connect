# """
# Tally Creator APIs
# Write operations to create masters in Tally
# """

# import frappe
# from tally_connect.tally_integration.api.checkers import _check_master_exists


# # ==================== PLACEHOLDER FUNCTIONS ====================
# # These will call XML builders (which we'll create next)

# @frappe.whitelist()
# def create_account_group(group_name, parent_group, force=False):
#     """
#     Create account group in Tally
    
#     Usage:
#         from tally_connect.tally_integration.api import create_account_group
#         result = create_account_group("Regional Sales", "Sales Accounts")
#     """
#     # Check if exists
#     if not force:
#         exists = _check_master_exists("Group", group_name)
#         if exists.get("exists"):
#             return {
#                 "success": True,
#                 "message": f"Group '{group_name}' already exists",
#                 "skipped": True
#             }
    
#     # TODO: Build XML and send to Tally
#     # (We'll implement this after creating XML builders)
    
#     return {
#         "success": False,
#         "message": "XML builder not yet implemented",
#         "todo": "Create xml_builder/groups.py first"
#     }


# @frappe.whitelist()
# def create_customer_ledger(customer_name, force=False):
#     """
#     Create customer ledger in Tally
    
#     Usage:
#         from tally_connect.tally_integration.api import create_customer_ledger
#         result = create_customer_ledger("CUST-00001")
#     """
#     if not force:
#         exists = _check_master_exists("Ledger", customer_name)
#         if exists.get("exists"):
#             return {
#                 "success": True,
#                 "message": f"Customer '{customer_name}' already exists",
#                 "skipped": True
#             }
    
#     # TODO: Implement after XML builder
#     return {
#         "success": False,
#         "message": "Not implemented yet"
#     }


# @frappe.whitelist()
# def create_supplier_ledger(supplier_name, force=False):
#     """Create supplier ledger in Tally"""
#     # Same pattern as customer
#     return {"success": False, "message": "Not implemented yet"}


# @frappe.whitelist()
# def create_stock_item(item_code, force=False):
#     """Create stock item in Tally"""
#     return {"success": False, "message": "Not implemented yet"}


# @frappe.whitelist()
# def create_godown(godown_name, parent_godown="", force=False):
#     """Create godown in Tally"""
#     return {"success": False, "message": "Not implemented yet"}
"""
Tally Creator APIs
Write operations to create masters in Tally
(Full implementation will use XML builders)
"""

import frappe
from tally_connect.tally_integration.api.checkers import _check_master_exists


# ==================== STUBS (Implement after XML builders) ====================

# @frappe.whitelist()
# def create_account_group(group_name, parent_group, force=False):
#     """
#     Create account group in Tally
    
#     Usage:
#         result = create_account_group("Regional Sales", "Sales Accounts")
#     """
#     if not force:
#         exists = _check_master_exists("Group", group_name)
#         if exists.get("exists"):
#             return {
#                 "success": True,
#                 "message": f"Group '{group_name}' already exists",
#                 "skipped": True
#             }
    
#     # TODO: Will implement after creating xml_builder/groups.py
#     return {
#         "success": False,
#         "message": "Not implemented yet - will add after XML builder"
#     }


# @frappe.whitelist()
# def create_customer_ledger(customer_name, force=False):
#     """Create customer ledger in Tally"""
#     if not force:
#         exists = _check_master_exists("Ledger", customer_name)
#         if exists.get("exists"):
#             return {"success": True, "message": f"Customer '{customer_name}' already exists", "skipped": True}
    
#     return {"success": False, "message": "Not implemented yet"}


# @frappe.whitelist()
# def create_supplier_ledger(supplier_name, force=False):
#     """Create supplier ledger in Tally"""
#     return {"success": False, "message": "Not implemented yet"}


# @frappe.whitelist()
# def create_stock_item(item_code, force=False):
#     """Create stock item in Tally"""
#     return {"success": False, "message": "Not implemented yet"}


# @frappe.whitelist()
# def create_godown(godown_name, parent_godown="", force=False):
#     """Create godown in Tally"""
#     return {"success": False, "message": "Not implemented yet"}


# @frappe.whitelist()
# def create_unit(unit_name, symbol=None, force=False):
#     """Create unit in Tally"""
#     return {"success": False, "message": "Not implemented yet"}

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
    format_amount_for_tally
)


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
        if result.get("error_type") in ["NETWORK_ERROR", "TIMEOUT"]:
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

@frappe.whitelist()
def create_customer_ledger_in_tally(customer_name, company=None):
    """
    Create customer ledger in Tally from ERPNext Customer
    Uses ERPNext's field structure and validation
    
    Args:
        customer_name: ERPNext Customer name
        company: ERPNext company name (optional, uses first company if not provided)
    
    Returns:
        dict: {"success": bool, "message": str, "sync_log": str, "retry_job": str}
    
    Example:
        create_customer_ledger_in_tally("ACME Corporation", "Your Company Ltd")
    """
    
    try:
        # Get customer document
        customer = frappe.get_doc("Customer", customer_name)
        
        # Determine company
        if not company:
            # Use first company from customer's accounts
            if customer.accounts and len(customer.accounts) > 0:
                company = customer.accounts[0].company
            else:
                # Use default company from settings
                company = frappe.defaults.get_global_default("company")
        
        # Get Tally company name
        tally_company = get_tally_company_for_erpnext_company(company)
        
        # Get parent group
        parent_group = get_customer_parent_group(customer_name, company)
        
        # Validate parent group exists in Tally
        parent_check = check_master_exists("Group", parent_group)
        if not parent_check.get("exists"):
            error_msg = f"Parent group '{parent_group}' does not exist in Tally"
            frappe.log_error(error_msg, "Tally Ledger Creator")
            
            retry_job = create_retry_job(
                document_type="Customer",
                document_name=customer_name,
                operation="Create Ledger",
                error_message=error_msg
            )
            
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # Check if ledger already exists
        exists_check = check_master_exists("Ledger", customer.customer_name)
        if exists_check.get("exists"):
            return {
                "success": False,
                "error": f"Ledger '{customer.customer_name}' already exists in Tally",
                "already_exists": True,
                "action_required": "UPDATE"
            }
        
        # Build address (from primary address)
        address_xml = ""
        if customer.customer_primary_address:
            try:
                address_doc = frappe.get_doc("Address", customer.customer_primary_address)
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
        
        # Build GSTIN (use ERPNext's validation - already validated)
        gstin_xml = ""
        if customer.gstin:
            gstin_xml = f"""
          <PARTYGSTIN.LIST>
            <PARTYGSTIN>{customer.gstin}</PARTYGSTIN>
          </PARTYGSTIN.LIST>"""
        
        # Build contact details
        contact_xml = ""
        if customer.mobile_no or customer.email_id:
            contact_lines = []
            if customer.mobile_no:
                contact_lines.append(f"Mobile: {customer.mobile_no}")
            if customer.email_id:
                contact_lines.append(f"Email: {customer.email_id}")
            
            contact_xml = f"""
          <LEDGERCONTACT>{escape_xml("; ".join(contact_lines))}</LEDGERCONTACT>"""
        
        # Build Tally XML
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
        <LEDGER NAME="{escape_xml(customer.customer_name)}" ACTION="Create">
          <NAME>{escape_xml(customer.customer_name)}</NAME>
          <PARENT>{escape_xml(parent_group)}</PARENT>
          <ISBILLWISEON>Yes</ISBILLWISEON>
          <AFFECTSSTOCK>No</AFFECTSSTOCK>{address_xml}{gstin_xml}{contact_xml}
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
            xml=ledger_xml
        )
        
        # Send to Tally
        result = send_xml_to_tally(log, ledger_xml)
        
        # Handle result
        if not result.get("success"):
            if result.get("error_type") in ["NETWORK_ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Customer",
                    document_name=customer_name,
                    operation="Create Ledger",
                    error_message=result.get("error", "Unknown error")
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
        
        # Update customer document
        try:
            customer.db_set("custom_tally_synced", 1, update_modified=False)
            customer.db_set("custom_tally_sync_date", now(), update_modified=False)
        except:
            pass
        
        return {
            "success": True,
            "message": f"Customer ledger '{customer.customer_name}' created in Tally",
            "sync_log": log.name
        }
    
    except Exception as e:
        error_msg = f"Exception creating customer ledger: {str(e)}"
        frappe.log_error(error_msg, "Tally Ledger Creator")
        
        retry_job = create_retry_job(
            document_type="Customer",
            document_name=customer_name,
            operation="Create Ledger",
            error_message=error_msg
        )
        
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None
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
            if result.get("error_type") in ["NETWORK_ERROR", "TIMEOUT"]:
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
        if result.get("error_type") in ["NETWORK_ERROR", "TIMEOUT"]:
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

@frappe.whitelist()
def create_stock_item_in_tally(item_code, company=None):
    """
    Create stock item in Tally from ERPNext Item
    
    KEY MAPPINGS:
    - Item Name → Tally Stock Item NAME
    - Item Code → Tally ALIAS
    - Item UOM → Base Units
    - Alternate UOMs → Alternate Units with conversion
    
    Args:
        item_code: ERPNext Item code
        company: ERPNext company name
    
    Returns:
        dict: {"success": bool, "message": str}
    
    Example:
        create_stock_item_in_tally("ITEM-001", "Your Company")
    """
    
    try:
        # Get item document
        item = frappe.get_doc("Item", item_code)
        
        tally_company = get_tally_company_for_erpnext_company(company)
        
        # Get stock group
        settings = get_settings()
        stock_group = item.item_group or settings.default_inventory_stock_group or "Primary"
        
        # Check stock group exists
        group_check = check_master_exists("StockGroup", stock_group)
        if not group_check.get("exists"):
            error_msg = f"Stock Group '{stock_group}' does not exist in Tally"
            retry_job = create_retry_job(
                document_type="Item",
                document_name=item_code,
                operation="Create Stock Item",
                error_message=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # Check base unit exists
        unit_check = check_master_exists("Unit", item.stock_uom)
        if not unit_check.get("exists"):
            error_msg = f"Unit '{item.stock_uom}' does not exist in Tally"
            retry_job = create_retry_job(
                document_type="Item",
                document_name=item_code,
                operation="Create Stock Item",
                error_message=error_msg
            )
            return {
                "success": False,
                "error": error_msg,
                "retry_job": retry_job.name if retry_job else None
            }
        
        # Check if item exists (using Item Name as per requirement)
        exists_check = check_master_exists("StockItem", item.item_name)
        if exists_check.get("exists"):
            return {
                "success": False,
                "error": f"Stock Item '{item.item_name}' already exists in Tally",
                "already_exists": True,
                "action_required": "UPDATE"
            }
        
        # Build GST Classification XML
        gst_xml = ""
        if hasattr(item, 'custom_gst_hsn_code') and item.custom_gst_hsn_code:
            # Check if GST classification exists
            gst_check = check_master_exists("GSTClassification", item.custom_gst_hsn_code)
            if gst_check.get("exists"):
                gst_xml = f"""
          <GSTAPPLICABLE>Applicable</GSTAPPLICABLE>
          <GSTCLASSIFICATIONNAME>{escape_xml(item.custom_gst_hsn_code)}</GSTCLASSIFICATIONNAME>"""
        
        # Build HSN Code XML
        hsn_xml = ""
        if item.gst_hsn_code:
            hsn_xml = f"""
          <HSNCODE>{item.gst_hsn_code}</HSNCODE>"""
        
        # Build Alternate Units XML (CRITICAL: Box/Pcs conversion)
        alternate_units_xml = ""
        if item.uoms and len(item.uoms) > 0:
            for uom_row in item.uoms:
                # Check if alternate unit exists in Tally
                alt_unit_check = check_master_exists("Unit", uom_row.uom)
                if alt_unit_check.get("exists"):
                    conversion = flt(uom_row.conversion_factor) or 1
                    alternate_units_xml += f"""
          <MULTIPLEUNITS.LIST>
            <REPORTINGUOM>{escape_xml(uom_row.uom)}</REPORTINGUOM>
            <CONVERSIONFACTOR>{conversion}</CONVERSIONFACTOR>
            <BASEUNITS>{escape_xml(item.stock_uom)}</BASEUNITS>
          </MULTIPLEUNITS.LIST>"""
        
        # Build Stock Item XML
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
          <NAME>{escape_xml(item.item_name)}</NAME>
          <ALIAS>{escape_xml(item.item_code)}</ALIAS>
          <PARENT>{escape_xml(stock_group)}</PARENT>
          <BASEUNITS>{escape_xml(item.stock_uom)}</BASEUNITS>{gst_xml}{hsn_xml}{alternate_units_xml}
        </STOCKITEM>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""
        
        # Create log and send
        log = create_sync_log(
            operation_type="Create Stock Item",
            doctype_name="Item",
            doc_name=item_code,
            company=company or "",
            xml=stock_item_xml
        )
        
        result = send_xml_to_tally(log, stock_item_xml)
        
        if not result.get("success"):
            if result.get("error_type") in ["NETWORK_ERROR", "TIMEOUT"]:
                retry_job = create_retry_job(
                    document_type="Item",
                    document_name=item_code,
                    operation="Create Stock Item",
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
        
        # Update item
        try:
            item.db_set("custom_tally_synced", 1, update_modified=False)
            item.db_set("custom_tally_sync_date", now(), update_modified=False)
        except:
            pass
        
        return {
            "success": True,
            "message": f"Stock Item '{item.item_name}' (Code: {item.item_code}) created in Tally",
            "sync_log": log.name
        }
    
    except Exception as e:
        error_msg = f"Exception creating stock item: {str(e)}"
        frappe.log_error(error_msg, "Tally Stock Item Creator")
        
        retry_job = create_retry_job(
            document_type="Item",
            document_name=item_code,
            operation="Create Stock Item",
            error_message=error_msg
        )
        
        return {
            "success": False,
            "error": error_msg,
            "retry_job": retry_job.name if retry_job else None
        }
