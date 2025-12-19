import frappe
from tally_connect.tally_integration.api.creators import create_customer_ledger_in_tally

# def create_customer_ledger_on_insert(doc, method=None):
#     # Optional: skip if already synced (e.g. during data import)
#     if getattr(doc, "custom_tally_synced", 0):
#         return

#     # Optional: only for specific companies or customer groups
#     # if doc.customer_group not in ("Q-Commerce", "Retail"):
#     #     return

#     res = create_customer_ledger_in_tally(doc.name, doc.company if hasattr(doc, "company") else None)

#     # Show non-blocking message; do NOT throw so insert succeeds even if Tally is down
#     if not res.get("success"):
#         frappe.msgprint(
#             f"Tally ledger not created for {doc.name}: {res.get('error')}",
#             indicator="orange",
#             alert=True,
#         )

import frappe
from tally_connect.tally_integration.api.creators import queue_customer_ledger_sync


def create_customer_ledger_on_insert(doc, method=None):
    """
    Queue Tally ledger creation after customer insert.
    Non-blocking: insert completes immediately even if Tally is down.
    """
    # Skip if already synced (e.g. during data import)
    if getattr(doc, "custom_tally_synced", 0):
        return

    # Optional: filter by customer group
    # if doc.customer_group not in ("Q-Commerce", "Retail"):
    #     return

    # Get company
    company = None
    if hasattr(doc, "accounts") and doc.accounts:
        company = doc.accounts[0].company
    elif hasattr(doc, "company"):
        company = doc.company
    
    # Enqueue background sync
    queue_customer_ledger_sync(doc.name, company)

    # Show non-blocking message
    frappe.msgprint(
        f"Customer '{doc.name}' created. Tally ledger sync queued.",
        indicator="green",
        alert=True,
    )
