# tally_connect/tally_integration/api/dependency_checker.py

import frappe
from frappe import _

def check_dependencies_for_document(doctype, docname, company):
    """
    Check if all dependencies exist in Tally for a document
    
    Returns:
        list: Missing masters [{"type": "Customer", "name": "ABC Corp", ...}]
    """
    missing = []
    
    if doctype == "Sales Invoice":
        missing = check_sales_invoice_dependencies(docname, company)
    elif doctype == "Purchase Invoice":
        missing = check_purchase_invoice_dependencies(docname, company)
    elif doctype == "Customer":
        missing = check_customer_dependencies(docname, company)
    elif doctype == "Item":
        missing = check_item_dependencies(docname, company)
    
    return missing


def check_sales_invoice_dependencies(invoice_name, company):
    """
    Check dependencies for Sales Invoice
    """
    from tally_connect.tally_integration.utils import check_master_exists
    
    invoice = frappe.get_doc("Sales Invoice", invoice_name)
    missing = []
    
    # Check customer
    result = check_master_exists("Ledger", invoice.customer)
    if not result.get("exists"):
        missing.append({
            "type": "Customer",
            "erpnext_doctype": "Customer",
            "name": invoice.customer,
            "display_name": invoice.customer_name or invoice.customer,
            "parent": get_customer_parent_group(invoice.customer, company)
        })
    
    # Check all items
    for item in invoice.items:
        result = check_master_exists("StockItem", item.item_code)
        if not result.get("exists"):
            missing.append({
                "type": "Item",
                "erpnext_doctype": "Item",
                "name": item.item_code,
                "display_name": item.item_name or item.item_code,
                "parent": get_item_parent_group(item.item_code, company)
            })
    
    return missing


def check_purchase_invoice_dependencies(invoice_name, company):
    """
    Check dependencies for Purchase Invoice
    """
    from tally_connect.tally_integration.utils import check_master_exists
    
    invoice = frappe.get_doc("Purchase Invoice", invoice_name)
    missing = []
    
    # Check supplier
    result = check_master_exists("Ledger", invoice.supplier)
    if not result.get("exists"):
        missing.append({
            "type": "Supplier",
            "erpnext_doctype": "Supplier",
            "name": invoice.supplier,
            "display_name": invoice.supplier_name or invoice.supplier,
            "parent": "Sundry Creditors"
        })
    
    # Check items
    for item in invoice.items:
        result = check_master_exists("StockItem", item.item_code)
        if not result.get("exists"):
            missing.append({
                "type": "Item",
                "erpnext_doctype": "Item",
                "name": item.item_code,
                "display_name": item.item_name or item.item_code,
                "parent": get_item_parent_group(item.item_code, company)
            })
    
    return missing


def check_customer_dependencies(customer_name, company):
    """
    Check dependencies for Customer
    """
    from tally_connect.tally_integration.utils import check_master_exists
    
    customer = frappe.get_doc("Customer", customer_name)
    missing = []
    
    # Check parent group
    parent_group = get_customer_parent_group(customer_name, company)
    result = check_master_exists("Group", parent_group)
    
    if not result.get("exists") and parent_group != "Sundry Debtors":
        missing.append({
            "type": "Group",
            "erpnext_doctype": None,
            "name": parent_group,
            "display_name": parent_group,
            "parent": "Sundry Debtors"
        })
    
    return missing


def check_item_dependencies(item_code, company):
    """
    Check dependencies for Item
    """
    from tally_connect.tally_integration.utils import check_master_exists, get_settings
    
    item = frappe.get_doc("Item", item_code)
    missing = []
    
    # Check stock group
    stock_group = get_item_parent_group(item_code, company)
    result = check_master_exists("StockGroup", stock_group)
    
    if not result.get("exists") and stock_group != "Primary":
        missing.append({
            "type": "Stock Group",
            "erpnext_doctype": None,
            "name": stock_group,
            "display_name": stock_group,
            "parent": "Primary"
        })
    
    # Check UOM
    result = check_master_exists("Unit", item.stock_uom)
    if not result.get("exists"):
        missing.append({
            "type": "Unit",
            "erpnext_doctype": "UOM",
            "name": item.stock_uom,
            "display_name": item.stock_uom,
            "parent": None
        })
    
    return missing


def get_customer_parent_group(customer_name, company):
    """
    Get parent group for customer ledger
    """
    from tally_connect.tally_integration.utils import get_settings
    settings = get_settings()
    
    # Try to get from customer's default account
    customer = frappe.get_doc("Customer", customer_name)
    for account in customer.accounts:
        if account.company == company and account.account:
            # Get parent account name
            parent_account = frappe.db.get_value("Account", account.account, "parent_account")
            if parent_account:
                return frappe.db.get_value("Account", parent_account, "account_name")
    
    # Fallback to settings
    return settings.default_customer_ledger or "Sundry Debtors"


def get_item_parent_group(item_code, company):
    """
    Get parent stock group for item
    """
    from tally_connect.tally_integration.utils import get_settings
    settings = get_settings()
    
    item = frappe.get_doc("Item", item_code)
    
    # Check if there's a mapping for this item group
    group_mapping = get_item_group_mapping()
    tally_group = group_mapping.get(item.item_group)
    
    if tally_group:
        return tally_group
    
    # Fallback to settings
    return settings.default_inventory_stock_group or "Primary"


def get_item_group_mapping():
    """
    Get mapping of ERPNext Item Groups to Tally Stock Groups
    """
    # TODO: Make this configurable in settings
    return {
        "Raw Material": "Raw Materials",
        "Finished Goods": "Finished Products",
        "Consumables": "Consumables",
        "Services": "Services"
    }
