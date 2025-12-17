# # """
# # Tally Validator APIs
# # Combined validation functions
# # """

import frappe
from tally_connect.tally_integration.api.checkers import (
    check_ledger_exists,
    check_group_exists,
    check_stock_item_exists
)
from tally_connect.tally_integration.utils import get_settings

from tally_connect.tally_integration.api.creators import (
    create_customer_ledger_in_tally,
    create_stock_group_in_tally,
    create_stock_item_in_tally,
    create_generic_ledger_in_tally,
)
# # @frappe.whitelist()
# # def validate_customer_for_tally(customer_name):
# #     """
# #     Validate if customer is ready to sync
    
# #     Returns:
# #         dict: {
# #             "valid": bool,
# #             "errors": [],
# #             "warnings": []
# #         }
# #     """
# #     settings = get_settings()
# #     errors = []
# #     warnings = []
    
# #     # Check parent group exists
# #     parent_group = settings.default_customer_ledger or "Sundry Debtors"
# #     group_check = check_group_exists(parent_group)
    
# #     if not group_check["exists"]:
# #         errors.append(f"Parent group '{parent_group}' not found in Tally")
    
# #     # Check if customer already exists
# #     ledger_check = check_ledger_exists(customer_name)
# #     if ledger_check["exists"]:
# #         warnings.append(f"Customer already exists in Tally")
    
# #     return {
# #         "valid": len(errors) == 0,
# #         "errors": errors,
# #         "warnings": warnings
# #     }


# # @frappe.whitelist()
# # def validate_item_for_tally(item_code):
# #     """Validate if item is ready to sync"""
# #     settings = get_settings()
# #     errors = []
# #     warnings = []
    
# #     # Check stock group
# #     stock_group = settings.default_inventory_stock_group or "Primary"
# #     group_check = check_group_exists(stock_group)
    
# #     if not group_check["exists"]:
# #         errors.append(f"Stock group '{stock_group}' not found in Tally")
    
# #     return {
# #         "valid": len(errors) == 0,
# #         "errors": errors,
# #         "warnings": warnings
# #     }


# # @frappe.whitelist()
# # def validate_invoice_for_tally(invoice_name):
# #     """Comprehensive validation for invoice"""
# #     # TODO: Implement full invoice validation
# #     return {
# #         "valid": False,
# #         "errors": ["Not implemented yet"],
# #         "warnings": []
# #     }
# """
# Tally Validator APIs
# Combined validation functions
# """

import frappe
from tally_connect.tally_integration.api.checkers import (
    check_ledger_exists,
    check_group_exists,
    check_stock_item_exists
)
from tally_connect.tally_integration.utils import get_settings


#     @frappe.whitelist()
#     def validate_customer_for_tally(customer_name):
#         """
#         Validate if customer is ready to sync
        
#         Returns:
#             dict: {"valid": bool, "errors": [...], "warnings": [...]}
#         """
#         settings = get_settings()
#         errors = []
#         warnings = []
        
#         # Check parent group exists
#         parent_group = settings.default_customer_ledger or "Sundry Debtors"
#         group_check = check_group_exists(parent_group)
        
#         if not group_check.get("exists"):
#             errors.append(f"Parent group '{parent_group}' not found in Tally")
        
#         # Check if customer already exists
#         ledger_check = check_ledger_exists(customer_name)
#         if ledger_check.get("exists"):
#             warnings.append(f"Customer already exists in Tally")
        
#         return {
#             "valid": len(errors) == 0,
#             "errors": errors,
#             "warnings": warnings
#         }


#     @frappe.whitelist()
#     def validate_item_for_tally(item_code):
#         """
#         Validate if item is ready to sync
#         """
#         settings = get_settings()
#         errors = []
#         warnings = []
        
#         # Check stock group
#         stock_group = settings.default_inventory_stock_group or "Primary"
#         group_check = check_group_exists(stock_group)
        
#         if not group_check.get("exists"):
#             errors.append(f"Stock group '{stock_group}' not found in Tally")
        
#         # Check if item already exists
#         item_check = check_stock_item_exists(item_code)
#         if item_check.get("exists"):
#             warnings.append(f"Item already exists in Tally")
        
#         return {
#             "valid": len(errors) == 0,
#             "errors": errors,
#             "warnings": warnings
#         }

"""
Tally Validator APIs
Combined validation + sync functions for:
- Customer / Item master validation
- Sales Order master validation
- Sales Invoice & Credit Note: validate masters, create missing, build XML, push
"""

import frappe

from tally_connect.tally_integration.api.checkers import (
    check_ledger_exists,
    check_group_exists,
    check_stock_item_exists,
    check_stock_group_exists,
)

from tally_connect.tally_integration.api.creators import (
    create_customer_ledger_in_tally,
    create_supplier_ledger_in_tally,
    create_stock_group_in_tally,
    create_stock_item_in_tally,
)

from tally_connect.tally_integration.utils import (
    get_settings,
    create_sync_log,
    send_xml_to_tally,
)



# ==================== BASIC MASTER VALIDATION (EXISTING) ====================

@frappe.whitelist()
def validate_customer_for_tally(customer_name):
    """
    Validate if customer is ready to sync.

    Returns:
        dict: {"valid": bool, "errors": [...], "warnings": [...]}
    """
    settings = get_settings()
    errors = []
    warnings = []

    # Check parent group exists
    parent_group = settings.default_customer_ledger or "Sundry Debtors"
    group_check = check_group_exists(parent_group)

    if not group_check.get("exists"):
        errors.append(f"Parent group '{parent_group}' not found in Tally")

    # Check if customer already exists
    ledger_check = check_ledger_exists(customer_name)
    if ledger_check.get("exists"):
        warnings.append("Customer already exists in Tally")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }


@frappe.whitelist()
def validate_item_for_tally(item_code):
    """
    Validate if item is ready to sync.
    """
    settings = get_settings()
    errors = []
    warnings = []

    # Check stock group
    stock_group = settings.default_inventory_stock_group or "Primary"
    group_check = check_group_exists(stock_group)

    if not group_check.get("exists"):
        errors.append(f"Stock group '{stock_group}' not found in Tally")

    # Check if item already exists
    item_check = check_stock_item_exists(item_code)
    if item_check.get("exists"):
        warnings.append("Item already exists in Tally")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }


# ==================== SALES ORDER VALIDATION (BLOCK IF MISSING) ====================

def validate_sales_order_masters(doc, method=None):
    """
    Sales Order: only validate masters, do NOT create.
    Block submit if anything is missing.
    """
    missing = []

    # Customer
    if hasattr(doc, "customer") and doc.customer:
        cust = check_ledger_exists(doc.customer)
        if not cust.get("exists"):
            missing.append(f"Customer: {doc.customer}")

    # Items
    if hasattr(doc, "items"):
        for row in doc.items:
            item_res = check_stock_item_exists(row.item_code)
            if not item_res.get("exists"):
                missing.append(f"Item: {row.item_code}")

    if missing:
        msg = "Tally masters missing:\n" + "\n".join(missing)
        frappe.throw(msg)


# ==================== XML HELPERS (SIMPLE V1) ====================

def _build_sales_invoice_xml(doc):
    """
    Simple Sales Invoice → Tally Sales voucher XML.
    V2 can switch to Field Mapping based XML.
    """
    posting_date = doc.posting_date.replace("-", "") if doc.posting_date else ""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Vouchers</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <VOUCHER>
          <VOUCHERTYPENAME>Sales</VOUCHERTYPENAME>
          <DATE>{posting_date}</DATE>
          <PARTYLEDGERNAME>{doc.customer}</PARTYLEDGERNAME>
          <VOUCHERNUMBER>{doc.name}</VOUCHERNUMBER>
          <ISCANCELLED>No</ISCANCELLED>
"""

    # Inventory lines
    xml += "          <ALLINVENTORYENTRIES.LIST>\n"
    for row in doc.items:
        xml += f"""            <INVENTORYENTRIES.LIST>
              <STOCKITEMNAME>{row.item_name}</STOCKITEMNAME>
              <ISDEEMEDPOSITIVE>No</ISDEEMEDPOSITIVE>
              <RATE>{row.rate}</RATE>
              <AMOUNT>{row.amount}</AMOUNT>
              <ACTUALQTY>{row.qty}</ACTUALQTY>
              <BILLEDQTY>{row.qty}</BILLEDQTY>
            </INVENTORYENTRIES.LIST>
"""
    xml += "          </ALLINVENTORYENTRIES.LIST>\n"

    xml += """        </VOUCHER>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""
    return xml


def _build_credit_note_xml(doc):
    """
    Simple Credit Note → Tally Credit Note voucher XML.
    """
    posting_date = doc.posting_date.replace("-", "") if doc.posting_date else ""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Import</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>All Vouchers</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <IMPORTDUPS>DUPIGNORE</IMPORTDUPS>
      </STATICVARIABLES>
    </DESC>
    <DATA>
      <TALLYMESSAGE>
        <VOUCHER>
          <VOUCHERTYPENAME>Credit Note</VOUCHERTYPENAME>
          <DATE>{posting_date}</DATE>
          <PARTYLEDGERNAME>{doc.customer}</PARTYLEDGERNAME>
          <VOUCHERNUMBER>{doc.name}</VOUCHERNUMBER>
          <ISCANCELLED>No</ISCANCELLED>
"""

    xml += "          <ALLINVENTORYENTRIES.LIST>\n"
    if hasattr(doc, "items"):
        for row in doc.items:
            xml += f"""            <INVENTORYENTRIES.LIST>
              <STOCKITEMNAME>{row.item_name}</STOCKITEMNAME>
              <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
              <RATE>{row.rate}</RATE>
              <AMOUNT>{row.amount}</AMOUNT>
              <ACTUALQTY>{row.qty}</ACTUALQTY>
              <BILLEDQTY>{row.qty}</BILLEDQTY>
            </INVENTORYENTRIES.LIST>
"""
    xml += "          </ALLINVENTORYENTRIES.LIST>\n"

    xml += """        </VOUCHER>
      </TALLYMESSAGE>
    </DATA>
  </BODY>
</ENVELOPE>"""
    return xml


# ==================== SALES INVOICE FLOW ====================

@frappe.whitelist()
def validate_and_sync_sales_invoice(invoice_name):
    """
    Sales Invoice FULL flow:
    - Validate masters (customer, stock groups, items)
    - Auto-create missing masters using creators.py
    - Build XML
    - Push to Tally
    """
    settings = get_settings()
    if not settings.enabled:
        return {"success": False, "error": "Tally integration is disabled in settings"}

    doc = frappe.get_doc("Sales Invoice", invoice_name)
    created = []
    errors = []

    # CUSTOMER LEDGER
    cust_res = check_ledger_exists(doc.customer)
    if not cust_res.get("exists"):
        res = createcustomerledgerintally(doc.customer, doc.company)
        if res.get("success"):
            created.append(f"Customer: {doc.customer}")
        else:
            errors.append(f"Customer '{doc.customer}': {res.get('error')}")

    # ITEMS & STOCK GROUPS
    for row in doc.items:
        stock_group = row.item_group or settings.default_inventory_stock_group or "Primary"

        # Stock group
        group_res = check_stock_group_exists(stock_group)
        if not group_res.get("exists"):
            gres = createstockgroupintally(stock_group, "Primary", doc.company)
            if gres.get("success"):
                created.append(f"Stock Group: {stock_group}")
            else:
                errors.append(f"Stock Group '{stock_group}': {gres.get('error')}")

        # Stock item (using item_name as master)
        item_res = check_stock_item_exists(row.item_name)
        if not item_res.get("exists"):
            ires = createstockitemintally(row.item_code, doc.company)
            if ires.get("success"):
                created.append(f"Item: {row.item_code}")
            else:
                errors.append(f"Item '{row.item_code}': {ires.get('error')}")

    if errors:
        # Still continue to try push, but report errors
        frappe.msgprint(
            "Some masters could not be created:\n" + "\n".join(errors),
            title="Tally Master Creation Issues",
            indicator="red",
        )

    # BUILD XML
    xml_payload = _build_sales_invoice_xml(doc)

    # LOG + SEND
    log = create_sync_log(
        operationtype="Create Sales Invoice",
        doctypename="Sales Invoice",
        docname=doc.name,
        company=doc.company,
        xml=xml_payload,
    )
    result = sendxmltotally(log, xml_payload)

    if result.get("success"):
        doc.db_set("custom_tally_synced", 1, update_modified=False)
        frappe.db.commit()
        frappe.msgprint(
            f"Sales Invoice {doc.name} synced to Tally.\nCreated masters: " +
            (", ".join(created) if created else "None"),
            title="Tally Sync Success",
            indicator="green",
        )
        return {
            "success": True,
            "sync_log": log.name,
            "created_masters": created,
        }

    frappe.msgprint(
        f"Failed to sync Sales Invoice {doc.name} to Tally: {result.get('error')}",
        title="Tally Sync Failed",
        indicator="red",
    )
    return {
        "success": False,
        "sync_log": log.name,
        "error": result.get("error"),
        "created_masters": created,
    }


# ==================== CREDIT NOTE FLOW ====================

@frappe.whitelist()
def validate_and_sync_credit_note(credit_note_name):
    """
    Credit Note FULL flow:
    - Validate masters (customer, stock groups, items)
    - Auto-create missing masters
    - Build XML
    - Push to Tally
    """
    settings = get_settings()
    if not settings.enabled:
        return {"success": False, "error": "Tally integration is disabled in settings"}

    doc = frappe.get_doc("Credit Note", credit_note_name)
    created = []
    errors = []

    # CUSTOMER
    cust_res = check_ledger_exists(doc.customer)
    if not cust_res.get("exists"):
        res = createcustomerledgerintally(doc.customer, doc.company)
        if res.get("success"):
            created.append(f"Customer: {doc.customer}")
        else:
            errors.append(f"Customer '{doc.customer}': {res.get('error')}")

    # ITEMS & STOCK GROUPS
    if hasattr(doc, "items"):
        for row in doc.items:
            stock_group = row.item_group or settings.default_inventory_stock_group or "Primary"

            group_res = check_stock_group_exists(stock_group)
            if not group_res.get("exists"):
                gres = createstockgroupintally(stock_group, "Primary", doc.company)
                if gres.get("success"):
                    created.append(f"Stock Group: {stock_group}")
                else:
                    errors.append(f"Stock Group '{stock_group}': {gres.get('error')}")

            item_res = check_stock_item_exists(row.item_name)
            if not item_res.get("exists"):
                ires = createstockitemintally(row.item_code, doc.company)
                if ires.get("success"):
                    created.append(f"Item: {row.item_code}")
                else:
                    errors.append(f"Item '{row.item_code}': {ires.get('error')}")

    if errors:
        frappe.msgprint(
            "Some masters could not be created:\n" + "\n".join(errors),
            title="Tally Master Creation Issues",
            indicator="red",
        )

    # BUILD XML
    xml_payload = _build_credit_note_xml(doc)

    # LOG + SEND
    log = create_sync_log(
        operationtype="Create Credit Note",
        doctypename="Credit Note",
        docname=doc.name,
        company=doc.company,
        xml=xml_payload,
    )
    result = sendxmltotally(log, xml_payload)

    if result.get("success"):
        if hasattr(doc, "custom_tally_synced"):
            doc.db_set("custom_tally_synced", 1, update_modified=False)
        frappe.db.commit()
        frappe.msgprint(
            f"Credit Note {doc.name} synced to Tally.\nCreated masters: " +
            (", ".join(created) if created else "None"),
            title="Tally Sync Success",
            indicator="green",
        )
        return {
            "success": True,
            "sync_log": log.name,
            "created_masters": created,
        }

    frappe.msgprint(
        f"Failed to sync Credit Note {doc.name} to Tally: {result.get('error')}",
        title="Tally Sync Failed",
        indicator="red",
    )
    return {
        "success": False,
        "sync_log": log.name,
        "error": result.get("error"),
        "created_masters": created,
    }

# @frappe.whitelist()
# def create_missing_masters_for_document(doctype, docname):
#     """
#     Generic API: check a document (SO, SI, CN) and auto-create missing masters in Tally.

#     - Customer ledger
#     - Stock group (based on item_group or default)
#     - Stock item (based on item_code)

#     Returns:
#         {
#             "success": bool,
#             "created": [...],
#             "errors": [...],
#         }
#     """

#     settings = get_settings()
#     if not settings.enabled:
#         return {
#             "success": False,
#             "created": [],
#             "errors": ["Tally integration is disabled in settings"],
#         }

#     doc = frappe.get_doc(doctype, docname)
#     created = []
#     errors = []

#     # 1. CUSTOMER LEDGER
#     if getattr(doc, "customer", None):
#         cust_res = check_ledger_exists(doc.customer)
#         if not cust_res.get("exists"):
#             res = create_customer_ledger_in_tally(doc.customer, doc.company)
#             if res.get("success"):
#                 created.append(f"Customer: {doc.customer}")
#             else:
#                 errors.append(f"Customer '{doc.customer}': {res.get('error')}")

#     # 2. ITEMS (STOCK GROUP + STOCK ITEM)
#     if hasattr(doc, "items"):
#         for row in doc.items:
#             # Determine stock group
#             stock_group = (
#                 getattr(row, "item_group", None)
#                 or getattr(settings, "default_inventory_stock_group", None)
#                 or "Primary"
#             )

#             # 2.a Stock Group
#             group_res = check_stock_group_exists(stock_group)
#             if not group_res.get("exists"):
#                 gres = create_stock_group_in_tally(stock_group, "Primary", doc.company)
#                 if gres.get("success"):
#                     created.append(f"Stock Group: {stock_group}")
#                 else:
#                     errors.append(f"Stock Group '{stock_group}': {gres.get('error')}")

#             # 2.b Stock Item
#             item_code = row.item_code
#             item_name_for_check = row.item_name or item_code

#             item_res = check_stock_item_exists(item_name_for_check)
#             if not item_res.get("exists"):
#                 ires = create_stock_item_in_tally(item_code, doc.company)
#                 if ires.get("success"):
#                     created.append(f"Item: {item_code}")
#                 else:
#                     errors.append(f"Item '{item_code}': {ires.get('error')}")

#     return {
#         "success": len(errors) == 0,
#         "created": created,
#         "errors": errors,
#     }

# def guess_parent_group_for_ledger(ledger_name, settings):
#     """
#     Decide parent group for a ledger.

#     Uses config when available; only guesses for common patterns.
#     Returns None if no safe mapping is found so caller can raise
#     a clear error instead of putting ledger under a wrong group.
#     """
#     name = (ledger_name or "").lower()

#     # Config-driven parents if present on settings DocType
#     sales_parent = getattr(settings, "sales_ledger_parent_group", None) or "Sales Accounts"
#     tax_parent = getattr(settings, "tax_ledger_parent_group", None) or "Duties & Taxes"
#     round_parent = getattr(settings, "round_off_parent_group", None) or "Indirect Expenses"

#     # Sales / revenue ledgers
#     if "sales" in name or "revenue" in name:
#         return sales_parent

#     # GST / tax / cess ledgers
#     if any(key in name for key in ["cgst", "sgst", "igst", "gst", "tax", "cess"]):
#         return tax_parent

#     # Round-off ledgers
#     if "round" in name:
#         return round_parent

#     # No safe guess
#     return None
def guess_parent_group_for_ledger(ledger_name, settings):
    """
    Decide parent group for a ledger.

    Uses config when available; only guesses for common patterns.
    Returns None if no safe mapping is found so caller can raise
    a clear error instead of putting ledger under a wrong group.
    """
    name = (ledger_name or "").lower()

    # Config-driven parents if present on settings DocType
    sales_parent = getattr(settings, "sales_ledger_parent_group", None) or "Sales Accounts"
    tax_parent = getattr(settings, "tax_ledger_parent_group", None) or "Duties & Taxes"
    round_parent = getattr(settings, "round_off_parent_group", None) or "Indirect Expenses"

    # Sales / revenue ledgers
    if "sales" in name or "revenue" in name:
        return sales_parent

    # GST / tax / cess ledgers
    if any(key in name for key in ["cgst", "sgst", "igst", "gst", "tax", "cess"]):
        return tax_parent

    # Round-off ledgers
    if "round" in name:
        return round_parent

    # No safe guess
    return None

@frappe.whitelist()
def create_missing_masters_for_document(doctype, docname):
    """
    Generic API: check a document (SO, SI, CN) and auto-create missing masters in Tally.

    Handles:
      - Customer ledger
      - Stock group (based on item_group or default)
      - Stock item (based on item_code)
      - For Sales Invoice: Sales / GST / Round-off ledgers

    Returns:
        {
            "success": bool,
            "created": [...],
            "errors": [...],
        }
    """
    settings = get_settings()
    if not settings.enabled:
        return {
            "success": False,
            "created": [],
            "errors": ["Tally integration is disabled in settings"],
        }

    doc = frappe.get_doc(doctype, docname)
    created = []
    errors = []

    # ---------- 1. CUSTOMER LEDGER ----------
    if getattr(doc, "customer", None):
        cust_res = check_ledger_exists(doc.customer)
        if not cust_res.get("exists"):
            res = create_customer_ledger_in_tally(doc.customer, doc.company)
            if res.get("success"):
                created.append(f"Customer: {doc.customer}")
            else:
                errors.append(f"Customer '{doc.customer}': {res.get('error')}")

    # ---------- 2. ITEMS (STOCK GROUP + STOCK ITEM) ----------
    if hasattr(doc, "items"):
        for row in doc.items:
            # 2.a Stock Group
            stock_group = (
                getattr(row, "item_group", None)
                or getattr(settings, "default_inventory_stock_group", None)
                or "Primary"
            )

            group_res = check_stock_group_exists(stock_group)
            if not group_res.get("exists"):
                gres = create_stock_group_in_tally(stock_group, "Primary", doc.company)
                if gres.get("success"):
                    created.append(f"Stock Group: {stock_group}")
                else:
                    errors.append(f"Stock Group '{stock_group}': {gres.get('error')}")

            # 2.b Stock Item
            item_code = row.item_code
            item_name_for_check = row.item_name or item_code

            item_res = check_stock_item_exists(item_name_for_check)
            if not item_res.get("exists"):
                ires = create_stock_item_in_tally(item_code, doc.company)
                if ires.get("success"):
                    created.append(f"Item: {item_code}")
                else:
                    errors.append(f"Item '{item_code}': {ires.get('error')}")

    # ---------- 3. LEDGERS FOR SALES INVOICE ----------
    if doctype == "Sales Invoice":
        required_ledgers = {
            "Sales": settings.sales_ledger_name or "SALES A/C",
            "CGST": settings.cgst_ledger_name or "CGST",
            "SGST": settings.sgst_ledger_name or "SGST",
            "IGST": settings.igst_ledger_name or "IGST",
            "Round Off": settings.round_off_ledger_name or "Round Off",
        }

        for kind, ledger in required_ledgers.items():
            if not ledger:
                continue

            res = check_ledger_exists(ledger)
            if res.get("exists"):
                continue

            parent = guess_parent_group_for_ledger(ledger, settings)
            if not parent:
                errors.append(
                    f"Cannot auto-create {kind} ledger '{ledger}' – no parent group mapping found"
                )
                continue

            cres = create_generic_ledger_in_tally(ledger, parent, doc.company)
            if cres.get("success"):
                created.append(f"Ledger: {ledger} (parent {parent})")
            else:
                errors.append(f"{kind} ledger '{ledger}': {cres.get('error')}")

    return {
        "success": len(errors) == 0,
        "created": created,
        "errors": errors,
    }
