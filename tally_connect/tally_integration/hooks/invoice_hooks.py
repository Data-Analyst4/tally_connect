# # =============================================================================
# # FILE: tally_connect/tally_integration/hooks/invoice_hooks.py
# #
# # PURPOSE: Document event hooks for Sales Order/Invoice
# # =============================================================================

import frappe
from frappe import _

# def check_dependencies_before_submit(doc, method):
#     """
#     before_submit hook
    
#     Shows warning if masters are missing in Tally
#     Does NOT block submission (just informs user)
#     """
    
#     from tally_connect.tally_integration.utils import get_settings, is_enabled
    
#     # Skip if integration disabled
#     if not is_enabled():
#         return
    
#     settings = get_settings()
    
#     # Check if sync enabled for this doctype
#     if doc.doctype == "Sales Order" and not settings.get("sync_sales_orders"):
#         return
    
#     if doc.doctype == "Sales Invoice" and not settings.get("sync_sales_invoices"):
#         return
    
#     # Check dependencies
#     from tally_connect.tally_integration.api.dependency_checker import check_dependencies_for_document
    
#     try:
#         missing = check_dependencies_for_document(
#             doctype=doc.doctype,
#             docname=doc.name,
#             company=doc.company
#         )
        
#         if missing:
#             # Store in doc meta for client-side access
#             doc._tally_missing_masters = missing
            
#             # Show warning message
#             master_names = [m["display_name"] for m in missing[:3]]
#             others_count = len(missing) - 3
            
#             msg = f"<b>Tally Integration:</b> {len(missing)} master(s) missing in Tally:<br>"
#             msg += "<ul>"
#             for name in master_names:
#                 msg += f"<li>{name}</li>"
#             if others_count > 0:
#                 msg += f"<li>...and {others_count} more</li>"
#             msg += "</ul>"
#             msg += "Document will be submitted, but Tally sync may fail until masters are created."
            
#             frappe.msgprint(
#                 msg,
#                 title="Tally Masters Missing",
#                 indicator="orange"
#             )
    
#     except Exception as e:
#         # Don't block submission if check fails
#         frappe.log_error(
#             f"Dependency check failed: {str(e)}",
#             "Tally Dependency Check"
#         )


# invoice_hooks.py - REPLACE check_dependencies_before_submit()

# tally_connect/tally_integration/hooks/invoice_hooks.py
"""
Sales Order before_submit: VALIDATE + AUTO-CREATE masters
"""
import frappe
from frappe import _
# tally_connect/tally_integration/hooks/invoice_hooks.py
# ⭐ EXACT REPLACEMENT - 25 lines

def check_dependencies_before_submit(doc, method):
    """
    Sales Order before_submit: AUTO-CREATE missing masters using EXISTING APIs
    """
    if doc.doctype != "Sales Order":
        return
        
    from tally_connect.tally_integration.utils import get_settings
    settings = get_settings()
    if not settings.get("validate_sales_orders", 1):
        return
    
    frappe.msgprint("🔄 Checking & creating Tally masters...", indicator="blue")
    
    # ⭐ CALL YOUR EXISTING MASTER CREATION API
    from tally_connect.tally_integration.api.validators import create_missing_masters_for_document
    
    try:
        result = create_missing_masters_for_document(
            doctype=doc.doctype, 
            docname=doc.name
        )
        
        # ⭐ SHOW RESULTS
        created = result.get("created", [])
        errors = result.get("errors", [])
        
        if result.get("success") or not errors:
            frappe.msgprint(
                f"✅ Created {len(created)} Tally masters!<br>Sales Order ready.",
                title="Tally Masters Created",
                indicator="green"
            )
        else:
            frappe.msgprint(
                f"⚠️ Created {len(created)}/{len(created)+len(errors)} masters<br>"
                f"Errors: {', '.join(errors[:2])}",
                title="Partial Success",
                indicator="orange"
            )
            
    except Exception as e:
        frappe.log_error(f"Tally SO Hook: {doc.name}\n{str(e)}", "Sales Order Master Creation")
        frappe.msgprint("⚠️ Validation ran but SO will submit anyway.", indicator="orange")

def _create_master_now(master, company):
    """
    IMMEDIATE master creation - no async, no retry jobs
    """
    master_type = master.get("master_type")
    master_name = master.get("master_name")
    
    try:
        from tally_connect.tally_integration.api.creators import (
            create_customer_ledger_in_tally,
            create_stock_group_in_tally,
            create_stock_item_in_tally,
            create_unit_in_tally
        )
        
        if master_type == "Customer":
            result = create_customer_ledger_in_tally(master_name, company)
        elif master_type == "Stock Group":
            parent_group = master.get("parent_group", "Primary")
            result = create_stock_group_in_tally(master_name, parent_group, company)
        elif master_type == "Stock Item":
            item_group = master.get("item_group", "Primary")
            result = create_stock_item_in_tally(master_name, item_group, company)
        elif master_type == "Unit":
            result = create_unit_in_tally(master_name)
        else:
            return {
                "success": False,
                "error": f"Unsupported master: {master_type}"
            }
        
        # Success: return simple message
        if result.get("success"):
            return {
                "success": True,
                "message": f"{master_type}: {master_name}"
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Unknown error")
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"{master_type}: {str(e)}"
        }
