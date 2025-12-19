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

import frappe
from frappe import _
# def queue_invoice_sync(doc, method):
#     """
#     Sales Invoice on_submit: auto-sync to Tally
#     NEVER blocks submission even if sync fails
#     """
#     try:
#         if doc.doctype != "Sales Invoice":
#             return

#         from tally_connect.tally_integration.utils import get_settings
#         settings = get_settings()
        
#         # Check if integration enabled
#         if not settings.enabled:
#             return
            
#         # Check if invoice sync enabled (optional setting check)
#         if not getattr(settings, "sync_sales_invoices", 1):
#             return

#         is_credit_note = bool(getattr(doc, "is_return", 0))

#         if is_credit_note:
#             # Credit Note
#             frappe.enqueue(
#                 "tally_connect.tally_integration.api.creators.create_credit_note_in_tally",
#                 queue="long",
#                 timeout=600,
#                 now=False,
#                 enqueue_after_commit=True,
#                 credit_note_name=doc.name,
#                 job_name=f"Tally Credit Note - {doc.name}",
#             )
#         else:
#             # Normal Invoice
#             frappe.enqueue(
#                 "tally_connect.tally_integration.api.creators.create_sales_invoice_in_tally",
#                 queue="long",
#                 timeout=600,
#                 now=False,
#                 enqueue_after_commit=True,
#                 invoice_name=doc.name,
#                 job_name=f"Tally Invoice - {doc.name}",
#             )
    
#     except Exception as e:
#         # Log error but do NOT block submission
#         frappe.log_error(
#             title=f"Tally Sync Hook Error: {doc.name}",
#             message=f"Failed to enqueue Tally sync for {doc.name}\n\nError: {str(e)}"
#         )
#         # Optionally show a non-blocking message to user
#         frappe.msgprint(
#             f"Invoice submitted successfully, but Tally sync could not be queued. Check Error Log.",
#             indicator="orange",
#             alert=True
#         )

def queue_invoice_sync(doc, method):
    try:
        frappe.enqueue(
            "tally_connect.tally_integration.api.creators.create_clean_sales_invoice_in_tally",
            queue="long",
            timeout=600,
            now=False,
            enqueue_after_commit=True,
            invoice_name=doc.name,
            job_name=f"Tally Invoice - {doc.name}",
        )
    except Exception as e:
        frappe.log_error(f"Failed to enqueue Tally sync for {doc.name}: {str(e)}",
                         "Tally Invoice Enqueue")
        frappe.msgprint(
            "Invoice submitted, but Tally sync could not be queued. Check Error Log.",
            alert=True,
            indicator="orange",
        )



import frappe
from tally_connect.tally_integration.api.creators import (
    queue_sales_invoice_or_return_sync,
)

# def queue_sales_invoice_sync_on_submit(doc, method):
#     # Safety checks
#     if doc.doctype != "Sales Invoice":
#         return
#     if doc.docstatus != 1:
#         return

#     # Optional: avoid double sync
#     # if getattr(doc, "custom_posted_to_tally", 0) and getattr(doc."custom_cn_to_tally", 0):
#     #     return

#     # Enqueue appropriate sync (invoice or credit note)
#     queue_sales_invoice_or_return_sync(doc.name)

#     # Optional user message
#     frappe.msgprint(
#         "Tally sync has been queued.",
#         alert=True,
#         indicator="green",
#     )

import frappe
from tally_connect.tally_integration.api.creators import (
    queue_sales_invoice_or_return_sync,
)


def queue_sales_invoice_sync_on_submit(doc, method):
    # Safety checks
    if doc.doctype != "Sales Invoice":
        return
    if doc.docstatus != 1:
        return

    # Check the right "already posted" field based on document type
    if getattr(doc, "is_return", 0):
        # For credit notes, check custom_cn_to_tally
        if getattr(doc, "custom_cn_to_tally", 0):
            return  # Already synced as credit note
    else:
        # For invoices, check custom_posted_to_tally
        if getattr(doc, "custom_posted_to_tally", 0):
            return  # Already synced as invoice

    # Enqueue appropriate sync (invoice or credit note)
    queue_sales_invoice_or_return_sync(doc.name)

    # Optional user message
    frappe.msgprint(
        "Tally sync has been queued.",
        alert=True,
        indicator="green",
    )
