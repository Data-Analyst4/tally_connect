import frappe
from tally_connect.tally_integration.api.creators import create_customer_ledger_in_tally

def create_customer_ledger_on_insert(doc, method=None):
    # Optional: skip if already synced (e.g. during data import)
    if getattr(doc, "custom_tally_synced", 0):
        return

    # Optional: only for specific companies or customer groups
    # if doc.customer_group not in ("Q-Commerce", "Retail"):
    #     return

    res = create_customer_ledger_in_tally(doc.name, doc.company if hasattr(doc, "company") else None)

    # Show non-blocking message; do NOT throw so insert succeeds even if Tally is down
    if not res.get("success"):
        frappe.msgprint(
            f"Tally ledger not created for {doc.name}: {res.get('error')}",
            indicator="orange",
            alert=True,
        )
