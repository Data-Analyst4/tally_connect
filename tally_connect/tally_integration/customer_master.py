"""
Customer master synchronization with Tally
Handles Customer → Tally Ledger creation
"""

import frappe
from xml.sax.saxutils import escape
from .utils import (
    get_settings,
    validate_tally_connection,
    create_sync_log,
    send_xml_to_tally
)

def get_customer_parent_group(customer_doc, company):
    """
    Get default receivable account for the company from customer's accounts table
    """
    # Customer has a child table 'accounts' with company-specific defaults
    for row in customer_doc.get("accounts", []):
        if row.company == company:
            if row.account:
                # Return the account name
                account_doc = frappe.get_doc("Account", row.account)
                return account_doc.account_name or row.account
    
    # Fallback to global setting
    settings = get_settings()
    return settings.customer_parent_group or "Sundry Debtors"

def customer_ledger_exists_in_tally(customer_name):
    """
    Query Tally to check if ledger exists
    For now, return False to force creation in first test
    """
    # TODO: implement actual Tally query
    return False

def build_customer_ledger_xml(customer_doc, parent_group, company):
    """
    Build Tally XML for customer ledger creation
    """
    name = escape(customer_doc.customer_name or customer_doc.name)
    parent = escape(parent_group)
    gstin = escape(customer_doc.gstin or "") if hasattr(customer_doc, 'gstin') else ""
    
    # Get primary address
    address_list = frappe.get_all(
        "Dynamic Link",
        filters={
            "link_doctype": "Customer",
            "link_name": customer_doc.name,
            "parenttype": "Address"
        },
        fields=["parent"],
        limit=1
    )
    
    address_line = ""
    state = ""
    if address_list:
        addr = frappe.get_doc("Address", address_list[0].parent)
        address_line = ", ".join(filter(None, [
            addr.address_line1,
            addr.address_line2,
            addr.city
        ]))
        state = addr.state or ""
    
    # Get primary contact
    contact_list = frappe.get_all(
        "Dynamic Link",
        filters={
            "link_doctype": "Customer",
            "link_name": customer_doc.name,
            "parenttype": "Contact"
        },
        fields=["parent"],
        limit=1
    )
    
    mobile = ""
    email = ""
    if contact_list:
        contact = frappe.get_doc("Contact", contact_list[0].parent)
        mobile = contact.mobile_no or contact.phone or ""
        email = contact.email_id or ""
    
    addr_xml = escape(address_line)
    state_xml = escape(state)
    mobile_xml = escape(mobile)
    email_xml = escape(email)
    
    return f"""<ENVELOPE>
  <HEADER>
    <TALLYREQUEST>Import Data</TALLYREQUEST>
  </HEADER>
  <BODY>
    <IMPORTDATA>
      <REQUESTDESC>
        <REPORTNAME>All Masters</REPORTNAME>
      </REQUESTDESC>
      <REQUESTDATA>
        <TALLYMESSAGE xmlns:UDF="TallyUDF">
          <LEDGER NAME="{name}" RESERVEDNAME="">
            <PARENT>{parent}</PARENT>
            <GSTREGISTRATIONTYPE>Regular</GSTREGISTRATIONTYPE>
            <PARTYGSTIN>{gstin}</PARTYGSTIN>
            <LEDGERPHONE>{mobile_xml}</LEDGERPHONE>
            <EMAIL>{email_xml}</EMAIL>
            <ADDRESS.LIST TYPE="String">
              <ADDRESS>{addr_xml}</ADDRESS>
            </ADDRESS.LIST>
            <STATENAME>{state_xml}</STATENAME>
            <COUNTRYNAME>India</COUNTRYNAME>
            <ISBILLWISEON>Yes</ISBILLWISEON>
            <ISCOSTCENTRESON>No</ISCOSTCENTRESON>
          </LEDGER>
        </TALLYMESSAGE>
      </REQUESTDATA>
    </IMPORTDATA>
  </BODY>
</ENVELOPE>"""

def check_or_create_customer_ledger(customer_name):
    """
    Main API: check if customer ledger exists in Tally, create if not
    """
    # Pre-flight validation
    validation = validate_tally_connection()
    if not validation["success"]:
        return {
            "success": False,
            "error": validation["error"],
            "validation_details": validation["checks"]
        }
    
    settings = get_settings()
    customer = frappe.get_doc("Customer", customer_name)
    company = settings.erpnext_company or customer.get("default_company")
    
    # Check existence in Tally
    if customer_ledger_exists_in_tally(customer_name):
        return {
            "success": True,
            "created": False,
            "ledger_name": customer.customer_name,
            "message": "Ledger already exists in Tally"
        }
    
    # Get parent group from customer's default receivable account
    parent_group = get_customer_parent_group(customer, company)
    
    # Build XML
    xml = build_customer_ledger_xml(customer, parent_group, company)
    
    # Create sync log
    log = create_sync_log(
        "CREATE_CUSTOMER_LEDGER",
        "Customer",
        customer_name,
        company,
        xml
    )
    
    # Send to Tally
    result = send_xml_to_tally(log, xml)
    
    # If successful, mark customer as synced
    if result.get("success"):
        frappe.db.set_value(
            "Customer",
            customer_name,
            {
                "tally_synced": 1,
                "tally_sync_date": frappe.utils.now()
            },
            update_modified=False
        )
        frappe.db.commit()
        
        result["ledger_name"] = customer.customer_name
        result["parent_group"] = parent_group
        result["sync_log_id"] = log.name
    
    return result
