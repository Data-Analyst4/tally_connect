"""
Tally Checker APIs
Read-only operations to check if masters/vouchers exist in Tally
"""

import frappe
from tally_connect.tally_integration.utils import check_master_exists


# ==================== PUBLIC WRAPPER FUNCTIONS ====================

@frappe.whitelist()
def check_ledger_exists(ledger_name):
    """
    Check if a ledger exists in Tally
    
    Usage:
        from tally_connect.tally_integration.api import check_ledger_exists
        result = check_ledger_exists("ACME Corp")
        if result['exists']:
            print("Found in Tally!")
    """
    return _check_master_exists("Ledger", ledger_name)


@frappe.whitelist()
def check_group_exists(group_name):
    """Check if an account group exists in Tally"""
    return _check_master_exists("Group", group_name)


@frappe.whitelist()
def check_stock_item_exists(item_code):
    """Check if a stock item exists in Tally"""
    return _check_master_exists("StockItem", item_code)


@frappe.whitelist()
def check_stock_group_exists(group_name):
    """Check if a stock group exists in Tally"""
    return _check_master_exists("StockGroup", group_name)


@frappe.whitelist()
def check_godown_exists(godown_name):
    """Check if a godown exists in Tally"""
    return _check_master_exists("Godown", godown_name)


@frappe.whitelist()
def check_unit_exists(unit_symbol):
    """Check if a unit exists in Tally"""
    return _check_master_exists("Unit", unit_symbol)

# Add this to your checkers.py

@frappe.whitelist()
def check_gst_classification_exists(classification_name):
    """Check if a GST Classification exists in Tally"""
    return _check_master_exists("GSTClassification", classification_name)

# ==================== INTERNAL HELPER ====================

def _check_master_exists(master_type, master_name):
    """
    Internal wrapper that calls utils.check_master_exists()
    Formats the response in a consistent, user-friendly way
    
    This is where the ACTUAL work happens (in utils.py)
    This function just makes the API cleaner
    """
    # Call your existing utils function
    result = check_master_exists(master_type, master_name)
    
    # Format response
    return {
        "exists": result.get("exists", False),
        "name": master_name,
        "master_type": master_type,
        "success": result.get("success", False),
        "error": result.get("error")
    }


# ==================== BATCH OPERATIONS ====================

@frappe.whitelist()
def batch_check_masters(master_type, names):
    """
    Check multiple masters at once
    
    Args:
        master_type: "Ledger", "StockItem", etc.
        names: JSON array of names or Python list
    
    Returns:
        dict: {
            "checked": 5,
            "existing": ["Item A", "Item B"],
            "missing": ["Item C", "Item D", "Item E"]
        }
    
    Usage:
        # From frontend
        frappe.call({
            method: 'tally_connect.tally_integration.api.checkers.batch_check_masters',
            args: {
                master_type: 'StockItem',
                names: ['ITEM-001', 'ITEM-002', 'ITEM-003']
            }
        })
        
        # From Python
        from tally_connect.tally_integration.api import batch_check_masters
        result = batch_check_masters('StockItem', ['ITEM-001', 'ITEM-002'])
    """
    import json
    
    # Handle JSON string input (from frontend)
    if isinstance(names, str):
        names = json.loads(names)
    
    existing = []
    missing = []
    
    for name in names:
        result = _check_master_exists(master_type, name)
        if result.get("exists"):
            existing.append(name)
        else:
            missing.append(name)
    
    return {
        "checked": len(names),
        "existing": existing,
        "missing": missing
    }


# ==================== DOCUMENT DEPENDENCY CHECKING ====================

@frappe.whitelist()
def check_document_dependencies(document_type, document_name):
    """
    Check if all dependencies exist for a document
    
    Usage:
        # Before syncing Sales Order
        from tally_connect.tally_integration.api import check_document_dependencies
        
        result = check_document_dependencies("Sales Order", "SO-2025-00001")
        
        if result['ready_to_sync']:
            # All masters exist, safe to sync
            sync_sales_order_to_tally(so.name)
        else:
            # Show what's missing
            frappe.msgprint(f"Missing: {result['missing_masters']}")
    """
    doc = frappe.get_doc(document_type, document_name)
    
    missing_masters = []
    existing_masters = []
    checks = {}
    
    # Check customer (for SO, SI, DN)
    if hasattr(doc, "customer"):
        result = check_ledger_exists(doc.customer)
        checks["customer"] = result
        
        if result.get("exists"):
            existing_masters.append(f"Customer: {doc.customer}")
        else:
            missing_masters.append(f"Customer: {doc.customer}")
    
    # Check supplier (for PO, PI)
    if hasattr(doc, "supplier"):
        result = check_ledger_exists(doc.supplier)
        checks["supplier"] = result
        
        if result.get("exists"):
            existing_masters.append(f"Supplier: {doc.supplier}")
        else:
            missing_masters.append(f"Supplier: {doc.supplier}")
    
    # Check items
    if hasattr(doc, "items"):
        checks["items"] = []
        for item in doc.items:
            result = check_stock_item_exists(item.item_code)
            checks["items"].append({
                "item_code": item.item_code,
                "exists": result.get("exists")
            })
            
            if result.get("exists"):
                existing_masters.append(f"Item: {item.item_code}")
            else:
                missing_masters.append(f"Item: {item.item_code}")
    
    return {
        "ready_to_sync": len(missing_masters) == 0,
        "missing_masters": missing_masters,
        "existing_masters": existing_masters,
        "checks": checks
    }


# ==================== COMPANY CHECKING ====================

# @frappe.whitelist()
# def check_tally_company():
#     """
#     Check if correct company is loaded in Tally
    
#     Usage:
#         from tally_connect.tally_integration.api import check_tally_company
        
#         result = check_tally_company()
#         if not result['matches']:
#             frappe.throw(f"Wrong company! Expected: {result['expected']}")
#     """
#     from tally_connect.tally_integration.utils import verify_tally_company
#     return verify_tally_company()

# Add these at the end of api/checkers.py

# ==================== VOUCHER CHECKING ====================

@frappe.whitelist()
def check_voucher_exists(voucher_number, voucher_type="Sales"):
    """
    Check if a voucher exists in Tally
    (Placeholder - will implement later)
    """
    return {
        "exists": False,
        "message": "Voucher checking not implemented yet"
    }


# ==================== COMPANY CHECKING ====================

@frappe.whitelist()
def check_tally_company():
    """
    Check if correct company is loaded
    (Wrapper for utils function)
    """
    from tally_connect.tally_integration.utils import verify_tally_company
    return verify_tally_company()

# ADD TO END OF checkers.py

@frappe.whitelist()
def check_dependencies_and_create_requests(doctype, docname, company):
    """
    Check dependencies and create requests for missing masters
    
    Called from: Document submit hooks
    
    Returns:
        dict: {
            "has_missing": bool,
            "missing_count": int,
            "requests_created": [request_names]
        }
    """
    from tally_connect.tally_integration.api.dependency_checker import check_dependencies_for_document
    
    missing = check_dependencies_for_document(doctype, docname, company)
    
    if not missing:
        return {
            "has_missing": False,
            "missing_count": 0
        }
    
    # Create requests for missing masters
    requests_created = []
    
    for master in missing:
        request = frappe.get_doc({
            "doctype": "Tally Master Creation Request",
            "master_type": master["type"],
            "erpnext_doctype": master["erpnext_doctype"],
            "erpnext_document": master["name"],
            "master_name": master["display_name"],
            "parent_group": master.get("parent"),
            "company": company,
            "linked_transaction": docname,
            "linked_transaction_doctype": doctype,
            "priority": "Normal"
        })
        request.insert(ignore_permissions=True)
        requests_created.append(request.name)
    
    frappe.db.commit()
    
    return {
        "has_missing": True,
        "missing_count": len(missing),
        "requests_created": requests_created,
        "missing_masters": missing
    }
