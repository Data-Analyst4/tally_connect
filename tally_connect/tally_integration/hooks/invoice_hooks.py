# =============================================================================
# FILE: tally_connect/tally_integration/hooks/invoice_hooks.py
#
# PURPOSE: Document event hooks for Sales Order/Invoice
# =============================================================================

import frappe
from frappe import _

def check_dependencies_before_submit(doc, method):
    """
    before_submit hook
    
    Shows warning if masters are missing in Tally
    Does NOT block submission (just informs user)
    """
    
    from tally_connect.tally_integration.utils import get_settings, is_enabled
    
    # Skip if integration disabled
    if not is_enabled():
        return
    
    settings = get_settings()
    
    # Check if sync enabled for this doctype
    if doc.doctype == "Sales Order" and not settings.get("sync_sales_orders"):
        return
    
    if doc.doctype == "Sales Invoice" and not settings.get("sync_sales_invoices"):
        return
    
    # Check dependencies
    from tally_connect.tally_integration.api.dependency_checker import check_dependencies_for_document
    
    try:
        missing = check_dependencies_for_document(
            doctype=doc.doctype,
            docname=doc.name,
            company=doc.company
        )
        
        if missing:
            # Store in doc meta for client-side access
            doc._tally_missing_masters = missing
            
            # Show warning message
            master_names = [m["display_name"] for m in missing[:3]]
            others_count = len(missing) - 3
            
            msg = f"<b>Tally Integration:</b> {len(missing)} master(s) missing in Tally:<br>"
            msg += "<ul>"
            for name in master_names:
                msg += f"<li>{name}</li>"
            if others_count > 0:
                msg += f"<li>...and {others_count} more</li>"
            msg += "</ul>"
            msg += "Document will be submitted, but Tally sync may fail until masters are created."
            
            frappe.msgprint(
                msg,
                title="Tally Masters Missing",
                indicator="orange"
            )
    
    except Exception as e:
        # Don't block submission if check fails
        frappe.log_error(
            f"Dependency check failed: {str(e)}",
            "Tally Dependency Check"
        )
