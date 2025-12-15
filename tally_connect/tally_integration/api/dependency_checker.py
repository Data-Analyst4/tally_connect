# # # tally_connect/tally_integration/api/dependency_checker.py

# # import frappe
# # from frappe import _

# # def check_dependencies_for_document(doctype, docname, company):
# #     """
# #     Check if all dependencies exist in Tally for a document
    
# #     Returns:
# #         list: Missing masters [{"type": "Customer", "name": "ABC Corp", ...}]
# #     """
# #     missing = []
    
# #     if doctype == "Sales Invoice":
# #         missing = check_sales_invoice_dependencies(docname, company)
# #     elif doctype == "Purchase Invoice":
# #         missing = check_purchase_invoice_dependencies(docname, company)
# #     elif doctype == "Customer":
# #         missing = check_customer_dependencies(docname, company)
# #     elif doctype == "Item":
# #         missing = check_item_dependencies(docname, company)
    
# #     return missing


# # def check_sales_invoice_dependencies(invoice_name, company):
# #     """
# #     Check dependencies for Sales Invoice
# #     """
# #     from tally_connect.tally_integration.utils import check_master_exists
    
# #     invoice = frappe.get_doc("Sales Invoice", invoice_name)
# #     missing = []
    
# #     # Check customer
# #     result = check_master_exists("Ledger", invoice.customer)
# #     if not result.get("exists"):
# #         missing.append({
# #             "type": "Customer",
# #             "erpnext_doctype": "Customer",
# #             "name": invoice.customer,
# #             "display_name": invoice.customer_name or invoice.customer,
# #             "parent": get_customer_parent_group(invoice.customer, company)
# #         })
    
# #     # Check all items
# #     for item in invoice.items:
# #         result = check_master_exists("StockItem", item.item_code)
# #         if not result.get("exists"):
# #             missing.append({
# #                 "type": "Item",
# #                 "erpnext_doctype": "Item",
# #                 "name": item.item_code,
# #                 "display_name": item.item_name or item.item_code,
# #                 "parent": get_item_parent_group(item.item_code, company)
# #             })
    
# #     return missing


# # def check_purchase_invoice_dependencies(invoice_name, company):
# #     """
# #     Check dependencies for Purchase Invoice
# #     """
# #     from tally_connect.tally_integration.utils import check_master_exists
    
# #     invoice = frappe.get_doc("Purchase Invoice", invoice_name)
# #     missing = []
    
# #     # Check supplier
# #     result = check_master_exists("Ledger", invoice.supplier)
# #     if not result.get("exists"):
# #         missing.append({
# #             "type": "Supplier",
# #             "erpnext_doctype": "Supplier",
# #             "name": invoice.supplier,
# #             "display_name": invoice.supplier_name or invoice.supplier,
# #             "parent": "Sundry Creditors"
# #         })
    
# #     # Check items
# #     for item in invoice.items:
# #         result = check_master_exists("StockItem", item.item_code)
# #         if not result.get("exists"):
# #             missing.append({
# #                 "type": "Item",
# #                 "erpnext_doctype": "Item",
# #                 "name": item.item_code,
# #                 "display_name": item.item_name or item.item_code,
# #                 "parent": get_item_parent_group(item.item_code, company)
# #             })
    
# #     return missing


# # def check_customer_dependencies(customer_name, company):
# #     """
# #     Check dependencies for Customer
# #     """
# #     from tally_connect.tally_integration.utils import check_master_exists
    
# #     customer = frappe.get_doc("Customer", customer_name)
# #     missing = []
    
# #     # Check parent group
# #     parent_group = get_customer_parent_group(customer_name, company)
# #     result = check_master_exists("Group", parent_group)
    
# #     if not result.get("exists") and parent_group != "Sundry Debtors":
# #         missing.append({
# #             "type": "Group",
# #             "erpnext_doctype": None,
# #             "name": parent_group,
# #             "display_name": parent_group,
# #             "parent": "Sundry Debtors"
# #         })
    
# #     return missing


# # def check_item_dependencies(item_code, company):
# #     """
# #     Check dependencies for Item
# #     """
# #     from tally_connect.tally_integration.utils import check_master_exists, get_settings
    
# #     item = frappe.get_doc("Item", item_code)
# #     missing = []
    
# #     # Check stock group
# #     stock_group = get_item_parent_group(item_code, company)
# #     result = check_master_exists("StockGroup", stock_group)
    
# #     if not result.get("exists") and stock_group != "Primary":
# #         missing.append({
# #             "type": "Stock Group",
# #             "erpnext_doctype": None,
# #             "name": stock_group,
# #             "display_name": stock_group,
# #             "parent": "Primary"
# #         })
    
# #     # Check UOM
# #     result = check_master_exists("Unit", item.stock_uom)
# #     if not result.get("exists"):
# #         missing.append({
# #             "type": "Unit",
# #             "erpnext_doctype": "UOM",
# #             "name": item.stock_uom,
# #             "display_name": item.stock_uom,
# #             "parent": None
# #         })
    
# #     return missing


# # def get_customer_parent_group(customer_name, company):
# #     """
# #     Get parent group for customer ledger
# #     """
# #     from tally_connect.tally_integration.utils import get_settings
# #     settings = get_settings()
    
# #     # Try to get from customer's default account
# #     customer = frappe.get_doc("Customer", customer_name)
# #     for account in customer.accounts:
# #         if account.company == company and account.account:
# #             # Get parent account name
# #             parent_account = frappe.db.get_value("Account", account.account, "parent_account")
# #             if parent_account:
# #                 return frappe.db.get_value("Account", parent_account, "account_name")
    
# #     # Fallback to settings
# #     return settings.default_customer_ledger or "Sundry Debtors"


# # def get_item_parent_group(item_code, company):
# #     """
# #     Get parent stock group for item
# #     """
# #     from tally_connect.tally_integration.utils import get_settings
# #     settings = get_settings()
    
# #     item = frappe.get_doc("Item", item_code)
    
# #     # Check if there's a mapping for this item group
# #     group_mapping = get_item_group_mapping()
# #     tally_group = group_mapping.get(item.item_group)
    
# #     if tally_group:
# #         return tally_group
    
# #     # Fallback to settings
# #     return settings.default_inventory_stock_group or "Primary"


# # def get_item_group_mapping():
# #     """
# #     Get mapping of ERPNext Item Groups to Tally Stock Groups
# #     """
# #     # TODO: Make this configurable in settings
# #     return {
# #         "Raw Material": "Raw Materials",
# #         "Finished Goods": "Finished Products",
# #         "Consumables": "Consumables",
# #         "Services": "Services"
# #     }

# # =============================================================================
# # FILE: tally_connect/tally_integration/api/dependency_checker.py
# #
# # PURPOSE: Pre-submission dependency validation
# #
# # WHAT THIS DOES:
# # - Before a transaction is submitted, check if all required masters exist
# # - If masters are missing, identify exactly what's missing
# # - Return list of missing masters with details
# #
# # WHY WE NEED THIS:
# # Before: Invoice submitted → Sync fails → User confused → Manual fix
# # After:  Check before submit → Show what's missing → Create requests → Sync succeeds
# #
# # HOW IT HELPS:
# # 1. Prevents sync failures (proactive vs reactive)
# # 2. Shows user exactly what's missing
# # 3. Allows bulk request creation
# # 4. Seamless user experience
# #
# # WHEN THIS RUNS:
# # - Before_submit hook on Sales Invoice, Purchase Invoice, etc.
# # - On-demand from UI ("Check Dependencies" button)
# #
# # FLOW:
# # User submits invoice → before_submit hook → check_dependencies
# #   → Missing masters found → Show dialog
# #   → User clicks "Request Approval" → Requests created
# #   → Invoice submit proceeds (will retry later)
# # =============================================================================

# import frappe
# from frappe import _
# from tally_connect.tally_integration.utils import check_master_exists

# # =============================================================================
# # MAIN ENTRY POINT
# # =============================================================================

# def check_dependencies_for_document(doctype, docname, company):
#     """
#     Check if all dependencies exist in Tally for a document
    
#     REAL-WORLD EXAMPLE:
#     ───────────────────────────────────────────────────────────────────
#     User submits Sales Invoice with:
#     - Customer: "ABC Corp"
#     - Items: ["Laptop", "Mouse", "Keyboard"]
    
#     This function checks Tally:
#     1. Does customer "ABC Corp" exist? → NO ❌
#     2. Does item "Laptop" exist? → YES ✓
#     3. Does item "Mouse" exist? → NO ❌
#     4. Does item "Keyboard" exist? → YES ✓
    
#     Returns: [
#         {"type": "Customer", "name": "ABC Corp", ...},
#         {"type": "Item", "name": "Mouse", ...}
#     ]
    
#     User sees: "2 masters missing. Click to create requests."
#     ───────────────────────────────────────────────────────────────────
    
#     Args:
#         doctype (str): Document type ("Sales Invoice", "Purchase Invoice", etc.)
#         docname (str): Document ID ("INV-001")
#         company (str): Company name ("Your Company")
    
#     Returns:
#         list: Missing masters with details
#         [
#             {
#                 "type": "Customer",           # Master type
#                 "erpnext_doctype": "Customer", # Source DocType
#                 "name": "CUST-001",           # ID
#                 "display_name": "ABC Corp",   # Human-readable name
#                 "parent": "Sundry Debtors"    # Parent group in Tally
#             },
#             ...
#         ]
    
#     WHY WE RETURN THIS STRUCTURE:
#     - type: To create correct master type request
#     - erpnext_doctype: To link to source document
#     - name: Document ID for data fetching
#     - display_name: User-friendly name for UI
#     - parent: To set parent group in Tally
    
#     HOW IT HELPS:
#     - User sees exactly what's missing (not just "sync failed")
#     - Can create all requests in one click
#     - Requests have all needed information pre-filled
#     """
    
#     missing = []
    
#     # -------------------------------------------------------------------------
#     # Route to appropriate checker based on document type
#     # -------------------------------------------------------------------------
#     # WHY: Different documents have different dependencies
#     # - Sales Invoice needs: Customer + Items
#     # - Purchase Invoice needs: Supplier + Items
#     # - Stock Entry needs: Items + Godowns
    
#     if doctype == "Sales Invoice":
#         missing = check_sales_invoice_dependencies(docname, company)
    
#     elif doctype == "Purchase Invoice":
#         missing = check_purchase_invoice_dependencies(docname, company)
    
#     elif doctype == "Stock Entry":
#         missing = check_stock_entry_dependencies(docname, company)
    
#     elif doctype == "Payment Entry":
#         missing = check_payment_entry_dependencies(docname, company)
    
#     # Add more document types as needed
    
#     return missing


# # =============================================================================
# # SALES INVOICE DEPENDENCY CHECKER
# # =============================================================================

# def check_sales_invoice_dependencies(invoice_name, company):
#     """
#     Check dependencies for Sales Invoice
    
#     DEPENDENCIES:
#     1. Customer → Must exist as Ledger in Tally
#     2. Items → Must exist as Stock Items in Tally
#     3. (Future) Units → Must exist as Units in Tally
    
#     Args:
#         invoice_name: Sales Invoice ID
#         company: Company name
    
#     Returns:
#         list: Missing masters
    
#     WHY WE CHECK EACH DEPENDENCY:
#     ────────────────────────────────────────────────────────────────
#     Customer: If missing, Tally rejects invoice with "Ledger not found"
#     Items: If missing, Tally rejects invoice with "Stock Item not found"
#     Units: If missing, Tally uses default unit (may be wrong)
#     ────────────────────────────────────────────────────────────────
#     """
    
#     # Load invoice document
#     invoice = frappe.get_doc("Sales Invoice", invoice_name)
#     missing = []
    
#     # =========================================================================
#     # CHECK 1: Customer ledger
#     # =========================================================================
#     # WHAT: Query Tally to see if customer exists
#     # WHY: Customer is mandatory for invoice voucher in Tally
#     # EDGE CASE: Customer might exist with different name
#     #            (We check exact name match for now)
    
#     customer_result = check_master_exists("Ledger", invoice.customer)
    
#     if not customer_result.get("exists"):
#         # Customer NOT found in Tally - Add to missing list
        
#         # Get parent group for this customer
#         # WHAT: Determines which ledger group in Tally (Sundry Debtors, etc.)
#         # WHY: Needed when creating the ledger
#         parent_group = get_customer_parent_group(invoice.customer, company)
        
#         missing.append({
#             "type": "Customer",                # Request will be for "Customer" type
#             "erpnext_doctype": "Customer",     # Source data from Customer doctype
#             "name": invoice.customer,           # Customer ID (e.g., "CUST-001")
#             "display_name": invoice.customer_name or invoice.customer,  # Human-readable
#             "parent": parent_group,             # "Sundry Debtors" or custom group
#             "priority": "High"                  # Customer is critical for invoice
#         })
    
#     # =========================================================================
#     # CHECK 2: Items
#     # =========================================================================
#     # WHAT: Loop through all items in invoice and check each one
#     # WHY: Even one missing item will cause sync to fail
#     # OPTIMIZATION: We could batch-check all items, but loop is clearer
    
#     for item_row in invoice.items:
#         item_code = item_row.item_code
        
#         # Query Tally for this item
#         item_result = check_master_exists("StockItem", item_code)
        
#         if not item_result.get("exists"):
#             # Item NOT found - Add to missing list
            
#             # Get stock group for this item
#             stock_group = get_item_stock_group(item_code, company)
            
#             missing.append({
#                 "type": "Item",
#                 "erpnext_doctype": "Item",
#                 "name": item_code,
#                 "display_name": item_row.item_name or item_code,
#                 "parent": stock_group,           # Stock group in Tally
#                 "priority": "Normal"              # Items are important but not critical
#             })
    
#     # =========================================================================
#     # CHECK 3: Tax ledgers (optional - for GST invoices)
#     # =========================================================================
#     # WHY: If invoice has GST, tax ledgers must exist in Tally
#     # EDGE CASE: Settings might not have tax ledger names configured
    
#     if invoice.taxes:
#         missing_tax_ledgers = check_tax_ledgers(invoice, company)
#         missing.extend(missing_tax_ledgers)
    
#     return missing


# # =============================================================================
# # PURCHASE INVOICE DEPENDENCY CHECKER
# # =============================================================================

# def check_purchase_invoice_dependencies(invoice_name, company):
#     """
#     Check dependencies for Purchase Invoice
    
#     DEPENDENCIES:
#     1. Supplier → Must exist as Ledger
#     2. Items → Must exist as Stock Items
    
#     Similar to Sales Invoice but with Supplier instead of Customer
#     """
    
#     invoice = frappe.get_doc("Purchase Invoice", invoice_name)
#     missing = []
    
#     # -------------------------------------------------------------------------
#     # Check supplier ledger
#     # -------------------------------------------------------------------------
#     supplier_result = check_master_exists("Ledger", invoice.supplier)
    
#     if not supplier_result.get("exists"):
#         parent_group = get_supplier_parent_group(invoice.supplier, company)
        
#         missing.append({
#             "type": "Supplier",
#             "erpnext_doctype": "Supplier",
#             "name": invoice.supplier,
#             "display_name": invoice.supplier_name or invoice.supplier,
#             "parent": parent_group,  # "Sundry Creditors"
#             "priority": "High"
#         })
    
#     # -------------------------------------------------------------------------
#     # Check items (same logic as sales invoice)
#     # -------------------------------------------------------------------------
#     for item_row in invoice.items:
#         item_code = item_row.item_code
#         item_result = check_master_exists("StockItem", item_code)
        
#         if not item_result.get("exists"):
#             stock_group = get_item_stock_group(item_code, company)
            
#             missing.append({
#                 "type": "Item",
#                 "erpnext_doctype": "Item",
#                 "name": item_code,
#                 "display_name": item_row.item_name or item_code,
#                 "parent": stock_group,
#                 "priority": "Normal"
#             })
    
#     return missing


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def get_customer_parent_group(customer_name, company):
#     """
#     Get parent ledger group for customer
    
#     LOGIC:
#     1. Check customer's default receivable account for this company
#     2. Get parent account name (this maps to Tally group)
#     3. Fallback to settings default
    
#     WHY THIS APPROACH:
#     - ERPNext has company-specific accounts
#     - Each customer can have different account per company
#     - Tally groups should match ERPNext account structure
    
#     EXAMPLE:
#     ───────────────────────────────────────────────────────────
#     Customer: "ABC Corp"
#     Company: "Your Company"
    
#     ERPNext Account Structure:
#     Assets
#       └── Current Assets
#             └── Debtors
#                   └── ABC Corp (Customer account)
    
#     This function returns: "Debtors"
#     In Tally, customer ledger will be under "Debtors" group
#     ───────────────────────────────────────────────────────────
    
#     Args:
#         customer_name: Customer document name
#         company: Company name
    
#     Returns:
#         str: Parent group name ("Sundry Debtors", "Debtors", etc.)
#     """
    
#     try:
#         # Get customer document
#         customer = frappe.get_doc("Customer", customer_name)
        
#         # Find account for this company
#         for account_row in customer.accounts:
#             if account_row.company == company and account_row.account:
#                 # Get the account document
#                 account_doc = frappe.get_doc("Account", account_row.account)
                
#                 # If account has parent, use parent's name
#                 if account_doc.parent_account:
#                     parent_account_doc = frappe.get_doc("Account", account_doc.parent_account)
#                     return parent_account_doc.account_name
                
#                 # No parent, use account name itself
#                 return account_doc.account_name
        
#         # No account found for this company - use settings default
#         from tally_connect.tally_integration.utils import get_settings
#         settings = get_settings()
#         return settings.default_customer_ledger or "Sundry Debtors"
    
#     except Exception as e:
#         # If anything goes wrong, return safe default
#         frappe.log_error(
#             f"Error getting customer parent group for {customer_name}: {str(e)}",
#             "Dependency Checker"
#         )
#         return "Sundry Debtors"


# def get_supplier_parent_group(supplier_name, company):
#     """
#     Get parent ledger group for supplier
    
#     Same logic as customer but uses payable account
#     Returns: "Sundry Creditors" or custom group
#     """
    
#     try:
#         supplier = frappe.get_doc("Supplier", supplier_name)
        
#         for account_row in supplier.accounts:
#             if account_row.company == company and account_row.account:
#                 account_doc = frappe.get_doc("Account", account_row.account)
                
#                 if account_doc.parent_account:
#                     parent_account_doc = frappe.get_doc("Account", account_doc.parent_account)
#                     return parent_account_doc.account_name
                
#                 return account_doc.account_name
        
#         from tally_connect.tally_integration.utils import get_settings
#         settings = get_settings()
#         return settings.default_supplier_ledger or "Sundry Creditors"
    
#     except Exception as e:
#         frappe.log_error(
#             f"Error getting supplier parent group for {supplier_name}: {str(e)}",
#             "Dependency Checker"
#         )
#         return "Sundry Creditors"


# def get_item_stock_group(item_code, company):
#     """
#     Get stock group for item
    
#     LOGIC:
#     1. Get item's item group from ERPNext
#     2. Check if there's a mapping from ERPNext item group to Tally stock group
#     3. Fallback to settings default
    
#     WHY MAPPING:
#     - ERPNext item groups may not match Tally stock groups
#     - Example: ERPNext "Finished Goods" → Tally "Finished Products"
#     - Allows flexible mapping
    
#     EXAMPLE MAPPING:
#     {
#         "Raw Material": "Raw Materials",
#         "Finished Goods": "Finished Products",
#         "Consumables": "Consumables",
#         "Services": "Services"
#     }
    
#     Args:
#         item_code: Item code
#         company: Company name
    
#     Returns:
#         str: Stock group name ("Primary", "Raw Materials", etc.)
#     """
    
#     try:
#         item = frappe.get_doc("Item", item_code)
        
#         # Get mapping from settings or custom logic
#         group_mapping = get_item_group_mapping()
        
#         # Check if this item group has a mapping
#         if item.item_group in group_mapping:
#             return group_mapping[item.item_group]
        
#         # No mapping - use settings default
#         from tally_connect.tally_integration.utils import get_settings
#         settings = get_settings()
#         return settings.default_inventory_stock_group or "Primary"
    
#     except Exception as e:
#         frappe.log_error(
#             f"Error getting item stock group for {item_code}: {str(e)}",
#             "Dependency Checker"
#         )
#         return "Primary"


# def get_item_group_mapping():
#     """
#     Get mapping of ERPNext Item Groups to Tally Stock Groups
    
#     TODO: Make this configurable in settings
#     For now, hardcoded common mappings
    
#     FUTURE ENHANCEMENT:
#     Add child table in Tally Integration Settings:
#     | ERPNext Item Group | Tally Stock Group |
#     |--------------------|-------------------|
#     | Raw Material       | Raw Materials     |
#     | Finished Goods     | Finished Products |
#     """
    
#     return {
#         "Raw Material": "Raw Materials",
#         "Finished Goods": "Finished Products",
#         "Consumables": "Consumables",
#         "Services": "Services",
#         "Work In Progress": "Work In Progress",
#         "Sub Assemblies": "Sub Assemblies"
#     }


# def check_tax_ledgers(invoice, company):
#     """
#     Check if tax ledgers exist in Tally
    
#     WHY: GST invoices need tax ledgers (CGST, SGST, IGST)
#     If missing, sync will fail
    
#     Args:
#         invoice: Invoice document
#         company: Company name
    
#     Returns:
#         list: Missing tax ledgers
#     """
    
#     from tally_connect.tally_integration.utils import get_settings
#     settings = get_settings()
    
#     missing = []
    
#     # Check CGST ledger
#     if settings.cgst_ledger_name:
#         result = check_master_exists("Ledger", settings.cgst_ledger_name)
#         if not result.get("exists"):
#             missing.append({
#                 "type": "Tax Ledger",
#                 "erpnext_doctype": None,  # No source document
#                 "name": settings.cgst_ledger_name,
#                 "display_name": settings.cgst_ledger_name,
#                 "parent": "Duties & Taxes",
#                 "priority": "High"
#             })
    
#     # Similar checks for SGST, IGST, etc.
    
#     return missing


# # =============================================================================
# # API ENDPOINT: Check dependencies on demand
# # =============================================================================

# @frappe.whitelist()
# def check_dependencies_and_show_dialog(doctype, docname, company):
#     """
#     API endpoint: Check dependencies and return for UI dialog
    
#     WHEN USED: User clicks "Check Dependencies" button before submitting
    
#     WHY: Allows user to proactively check before submission
#     Prevents surprise failures
    
#     Returns:
#         dict: {
#             "has_missing": bool,
#             "missing_count": int,
#             "missing_masters": [...],
#             "ready_to_sync": bool
#         }
    
#     UI USAGE:
#     ─────────
#     frappe.call({
#         method: 'check_dependencies_and_show_dialog',
#         args: {
#             doctype: 'Sales Invoice',
#             docname: 'INV-001',
#             company: 'Your Company'
#         },
#         callback: function(r) {
#             if (r.message.has_missing) {
#                 // Show dialog with missing masters
#                 show_create_requests_dialog(r.message.missing_masters);
#             } else {
#                 frappe.msgprint('All dependencies exist!');
#             }
#         }
#     });
#     """
    
#     missing = check_dependencies_for_document(doctype, docname, company)
    
#     return {
#         "has_missing": len(missing) > 0,
#         "missing_count": len(missing),
#         "missing_masters": missing,
#         "ready_to_sync": len(missing) == 0
#     }


# # =============================================================================
# # SUMMARY: WHAT THIS FILE DOES
# # =============================================================================
# """
# BEFORE THIS FILE EXISTED:
# ─────────────────────────────────────────────────────────────────────
# 1. User submits Sales Invoice
# 2. Sync job runs in background
# 3. Tally rejects: "Customer ABC Corp not found"
# 4. User sees error in sync log
# 5. User confused: "What do I do now?"
# 6. User calls support: "My invoice didn't sync"
# 7. Support: "You need to create customer in Tally first"
# 8. User: "How do I do that?"
# 9. ... Manual process ...
# 10. Eventually invoice syncs
# ─────────────────────────────────────────────────────────────────────

# AFTER THIS FILE:
# ─────────────────────────────────────────────────────────────────────
# 1. User submits Sales Invoice
# 2. before_submit hook checks dependencies
# 3. Dialog shows: "Customer ABC Corp missing. Create request?"
# 4. User clicks "Yes"
# 5. Request created and sent to admin
# 6. Admin approves
# 7. Customer auto-created in Tally
# 8. Invoice auto-retried
# 9. Invoice syncs successfully
# 10. User sees: "Invoice synced to Tally" ✓
# ─────────────────────────────────────────────────────────────────────

# BENEFITS:
# ✅ Proactive (prevents failures)
# ✅ User-friendly (clear guidance)
# ✅ Automated (minimal manual work)
# ✅ Audit trail (who requested what)
# ✅ Controlled (admin approval)
# ✅ Seamless (auto-retry after creation)
# """


# def check_sales_document_dependencies(docname, doctype, company):
#     """
#     Check dependencies for Sales Order/Invoice
    
#     Checks:
#     1. Customer ledger exists
#     2. All items exist
#     3. Tax ledgers exist (optional for now)
#     """
    
#     doc = frappe.get_doc(doctype, docname)
#     missing = []
    
#     # -------------------------------------------------------------------------
#     # CHECK 1: Customer Ledger
#     # -------------------------------------------------------------------------
#     customer_name = doc.customer
    
#     result = check_master_exists("Ledger", customer_name)
    
#     if not result.get("exists"):
#         # Customer NOT in Tally - add to missing list
#         parent_group = get_customer_parent_group(customer_name, company)
        
#         missing.append({
#             "type": "Customer",
#             "erpnext_doctype": "Customer",
#             "name": customer_name,
#             "display_name": doc.customer_name or customer_name,
#             "parent": parent_group,
#             "priority": "High"
#         })
    
#     # -------------------------------------------------------------------------
#     # CHECK 2: Stock Items
#     # -------------------------------------------------------------------------
#     for item_row in doc.items:
#         item_code = item_row.item_code
        
#         # Check if item is a stock item (skip services)
#         item_doc = frappe.get_cached_doc("Item", item_code)
#         if not item_doc.is_stock_item:
#             continue  # Skip services
        
#         result = check_master_exists("StockItem", item_code)
        
#         if not result.get("exists"):
#             # Item NOT in Tally
#             stock_group = get_item_stock_group(item_code, company)
            
#             missing.append({
#                 "type": "Item",
#                 "erpnext_doctype": "Item",
#                 "name": item_code,
#                 "display_name": item_row.item_name or item_code,
#                 "parent": stock_group,
#                 "priority": "Normal"
#             })
    
#     return missing

# # v2

# # =============================================================================
# # FILE: tally_connect/tally_integration/api/dependency_checker.py
# #
# # PURPOSE: Check if all Tally masters exist before document submission
# # CALLED FROM: Sales Order/Invoice before_submit hook
# # =============================================================================

# import frappe
# from frappe import _
# from tally_connect.tally_integration.utils import check_master_exists

# # =============================================================================
# # MAIN ENTRY POINT
# # =============================================================================

# def check_dependencies_for_document(doctype, docname, company):
#     """
#     Check if all dependencies exist in Tally for a document
    
#     Args:
#         doctype: "Sales Order", "Sales Invoice", etc.
#         docname: Document ID
#         company: Company name
    
#     Returns:
#         list: Missing masters
#         [
#             {
#                 "type": "Customer",
#                 "erpnext_doctype": "Customer",
#                 "name": "CUST-001",
#                 "display_name": "ABC Corp",
#                 "parent": "Sundry Debtors"
#             },
#             ...
#         ]
#     """
    
#     missing = []
    
#     if doctype in ["Sales Order", "Sales Invoice"]:
#         missing = check_sales_document_dependencies(docname, doctype, company)
    
#     elif doctype in ["Purchase Order", "Purchase Invoice"]:
#         missing = check_purchase_document_dependencies(docname, doctype, company)
    
#     elif doctype == "Payment Entry":
#         missing = check_payment_entry_dependencies(docname, company)
    
#     return missing


# # =============================================================================
# # SALES DOCUMENT CHECKER
# # =============================================================================

# def check_sales_document_dependencies(docname, doctype, company):
#     """
#     Check dependencies for Sales Order/Invoice
    
#     Checks:
#     1. Customer ledger exists
#     2. All items exist
#     3. Tax ledgers exist (optional for now)
#     """
    
#     doc = frappe.get_doc(doctype, docname)
#     missing = []
    
#     # -------------------------------------------------------------------------
#     # CHECK 1: Customer Ledger
#     # -------------------------------------------------------------------------
#     customer_name = doc.customer
    
#     result = check_master_exists("Ledger", customer_name)
    
#     if not result.get("exists"):
#         # Customer NOT in Tally - add to missing list
#         parent_group = get_customer_parent_group(customer_name, company)
        
#         missing.append({
#             "type": "Customer",
#             "erpnext_doctype": "Customer",
#             "name": customer_name,
#             "display_name": doc.customer_name or customer_name,
#             "parent": parent_group,
#             "priority": "High"
#         })
    
#     # -------------------------------------------------------------------------
#     # CHECK 2: Stock Items
#     # -------------------------------------------------------------------------
#     for item_row in doc.items:
#         item_code = item_row.item_code
        
#         # Check if item is a stock item (skip services)
#         item_doc = frappe.get_cached_doc("Item", item_code)
#         if not item_doc.is_stock_item:
#             continue  # Skip services
        
#         result = check_master_exists("StockItem", item_code)
        
#         if not result.get("exists"):
#             # Item NOT in Tally
#             stock_group = get_item_stock_group(item_code, company)
            
#             missing.append({
#                 "type": "Item",
#                 "erpnext_doctype": "Item",
#                 "name": item_code,
#                 "display_name": item_row.item_name or item_code,
#                 "parent": stock_group,
#                 "priority": "Normal"
#             })
    
#     return missing


# # =============================================================================
# # PURCHASE DOCUMENT CHECKER
# # =============================================================================

# def check_purchase_document_dependencies(docname, doctype, company):
#     """Check dependencies for Purchase Order/Invoice"""
    
#     doc = frappe.get_doc(doctype, docname)
#     missing = []
    
#     # Check supplier
#     supplier_name = doc.supplier
#     result = check_master_exists("Ledger", supplier_name)
    
#     if not result.get("exists"):
#         parent_group = get_supplier_parent_group(supplier_name, company)
        
#         missing.append({
#             "type": "Supplier",
#             "erpnext_doctype": "Supplier",
#             "name": supplier_name,
#             "display_name": doc.supplier_name or supplier_name,
#             "parent": parent_group,
#             "priority": "High"
#         })
    
#     # Check items (same logic as sales)
#     for item_row in doc.items:
#         item_code = item_row.item_code
#         item_doc = frappe.get_cached_doc("Item", item_code)
        
#         if not item_doc.is_stock_item:
#             continue
        
#         result = check_master_exists("StockItem", item_code)
        
#         if not result.get("exists"):
#             stock_group = get_item_stock_group(item_code, company)
            
#             missing.append({
#                 "type": "Item",
#                 "erpnext_doctype": "Item",
#                 "name": item_code,
#                 "display_name": item_row.item_name or item_code,
#                 "parent": stock_group,
#                 "priority": "Normal"
#             })
    
#     return missing


# # =============================================================================
# # PAYMENT ENTRY CHECKER
# # =============================================================================

# def check_payment_entry_dependencies(docname, company):
#     """Check dependencies for Payment Entry"""
    
#     payment = frappe.get_doc("Payment Entry", docname)
#     missing = []
    
#     # Check party (customer or supplier)
#     if payment.party_type == "Customer":
#         result = check_master_exists("Ledger", payment.party)
#         if not result.get("exists"):
#             parent_group = get_customer_parent_group(payment.party, company)
#             missing.append({
#                 "type": "Customer",
#                 "erpnext_doctype": "Customer",
#                 "name": payment.party,
#                 "display_name": payment.party_name or payment.party,
#                 "parent": parent_group,
#                 "priority": "High"
#             })
    
#     elif payment.party_type == "Supplier":
#         result = check_master_exists("Ledger", payment.party)
#         if not result.get("exists"):
#             parent_group = get_supplier_parent_group(payment.party, company)
#             missing.append({
#                 "type": "Supplier",
#                 "erpnext_doctype": "Supplier",
#                 "name": payment.party,
#                 "display_name": payment.party_name or payment.party,
#                 "parent": parent_group,
#                 "priority": "High"
#             })
    
#     return missing


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def get_customer_parent_group(customer_name, company):
#     """Get parent ledger group for customer"""
    
#     try:
#         from tally_connect.tally_integration.utils import get_settings
        
#         customer = frappe.get_doc("Customer", customer_name)
        
#         # Find account for this company
#         for account_row in customer.accounts:
#             if account_row.company == company and account_row.account:
#                 account_doc = frappe.get_doc("Account", account_row.account)
                
#                 if account_doc.parent_account:
#                     parent_account = frappe.get_doc("Account", account_doc.parent_account)
#                     return parent_account.account_name
                
#                 return account_doc.account_name
        
#         # Fallback to settings
#         settings = get_settings()
#         return settings.default_customer_ledger or "Sundry Debtors"
    
#     except Exception as e:
#         frappe.log_error(f"Error getting customer parent: {str(e)}", "Dependency Checker")
#         return "Sundry Debtors"


# def get_supplier_parent_group(supplier_name, company):
#     """Get parent ledger group for supplier"""
    
#     try:
#         from tally_connect.tally_integration.utils import get_settings
        
#         supplier = frappe.get_doc("Supplier", supplier_name)
        
#         for account_row in supplier.accounts:
#             if account_row.company == company and account_row.account:
#                 account_doc = frappe.get_doc("Account", account_row.account)
                
#                 if account_doc.parent_account:
#                     parent_account = frappe.get_doc("Account", account_doc.parent_account)
#                     return parent_account.account_name
                
#                 return account_doc.account_name
        
#         settings = get_settings()
#         return settings.default_supplier_ledger or "Sundry Creditors"
    
#     except Exception as e:
#         frappe.log_error(f"Error getting supplier parent: {str(e)}", "Dependency Checker")
#         return "Sundry Creditors"


# def get_item_stock_group(item_code, company):
#     """Get stock group for item"""
    
#     try:
#         from tally_connect.tally_integration.utils import get_settings
        
#         item = frappe.get_doc("Item", item_code)
        
#         # Simple mapping (can be enhanced later)
#         group_mapping = {
#             "Raw Material": "Raw Materials",
#             "Finished Goods": "Finished Products",
#             "Consumables": "Consumables",
#             "Services": "Services"
#         }
        
#         if item.item_group in group_mapping:
#             return group_mapping[item.item_group]
        
#         settings = get_settings()
#         return settings.default_inventory_stock_group or "Primary"
    
#     except Exception as e:
#         frappe.log_error(f"Error getting item stock group: {str(e)}", "Dependency Checker")
#         return "Primary"


# # =============================================================================
# # API ENDPOINT: Check Dependencies (Called from UI)
# # =============================================================================

# @frappe.whitelist()
# def check_dependencies_and_show_missing(doctype, docname, company):
#     """
#     API endpoint: Check dependencies and return missing masters
    
#     Called from: Client-side JavaScript (button click)
    
#     Returns:
#         dict: {
#             "has_missing": bool,
#             "missing_count": int,
#             "missing_masters": [...]
#         }
#     """
    
#     missing = check_dependencies_for_document(doctype, docname, company)
    
#     return {
#         "has_missing": len(missing) > 0,
#         "missing_count": len(missing),
#         "missing_masters": missing
#     }


# # =============================================================================
# # API ENDPOINT: Create Requests for Missing Masters
# # =============================================================================

# @frappe.whitelist()
# def create_requests_for_missing_masters(doctype, docname, company, missing_masters_json):
#     """
#     Create Tally Master Creation Requests for missing masters
    
#     Called from: Client-side dialog (after user confirms)
    
#     Args:
#         doctype: Source document type
#         docname: Source document ID
#         company: Company
#         missing_masters_json: JSON string of missing masters
    
#     Returns:
#         dict: {
#             "success": bool,
#             "requests_created": [request_names],
#             "message": str
#         }
#     """
    
#     import json
    
#     # Parse missing masters
#     missing_masters = json.loads(missing_masters_json)
    
#     requests_created = []
    
#     for master in missing_masters:
#         # Check if request already exists
#         existing = frappe.db.exists(
#             "Tally Master Creation Request",
#             {
#                 "erpnext_document": master["name"],
#                 "status": ["in", ["Pending Approval", "Approved", "In Progress"]]
#             }
#         )
        
#         if existing:
#             requests_created.append(existing)
#             continue
        
#         # Create new request
#         try:
#             request = frappe.get_doc({
#                 "doctype": "Tally Master Creation Request",
#                 "master_type": master["type"],
#                 "erpnext_doctype": master["erpnext_doctype"],
#                 "erpnext_document": master["name"],
#                 "master_name": master["display_name"],
#                 "parent_group": master["parent"],
#                 "company": company,
#                 "linked_transaction": docname,
#                 "linked_transaction_doctype": doctype,
#                 "priority": master.get("priority", "Normal"),
#                 "status": "Pending Approval",
#                 "requested_by": frappe.session.user
#             })
#             request.insert(ignore_permissions=True)
#             requests_created.append(request.name)
        
#         except Exception as e:
#             frappe.log_error(
#                 f"Failed to create request for {master['name']}: {str(e)}",
#                 "Dependency Checker"
#             )
    
#     frappe.db.commit()
    
#     return {
#         "success": True,
#         "requests_created": requests_created,
#         "message": f"Created {len(requests_created)} master creation request(s)"
#     }

import frappe
from frappe import _
from tally_connect.tally_integration.utils import check_master_exists

def check_dependencies_for_document(doctype, docname, company):
    """Check if all dependencies exist in Tally"""
    missing = []
    
    if doctype in ["Sales Order", "Sales Invoice"]:
        missing = check_sales_invoice_dependencies(docname, doctype, company)
    elif doctype in ["Purchase Order", "Purchase Invoice"]:
        missing = check_purchase_invoice_dependencies(docname, doctype, company)
    
    return missing

def check_sales_invoice_dependencies(docname, doctype, company):
    """Check dependencies for Sales Invoice/Order"""
    doc = frappe.get_doc(doctype, docname)
    missing = []
    
    # Check customer - VERIFY IT EXISTS IN ERPNEXT FIRST
    customer_exists_erpnext = frappe.db.exists("Customer", doc.customer)
    
    if customer_exists_erpnext:
        result = check_master_exists("Ledger", doc.customer)
        if not result.get("exists"):
            missing.append({
                "type": "Customer",
                "erpnext_doctype": "Customer",
                "name": doc.customer,
                "display_name": doc.customer_name or doc.customer,
                "parent": get_customer_parent_group(doc.customer, company),
                "priority": "High"
            })
    else:
        frappe.log_error(
            f"Customer '{doc.customer}' not found in ERPNext. Cannot create Tally request.",
            "Dependency Checker - Customer Not Found"
        )
    
    # Check items - VERIFY THEY EXIST IN ERPNEXT
    for item in doc.items:
        item_exists_erpnext = frappe.db.exists("Item", item.item_code)
        
        if item_exists_erpnext:
            result = check_master_exists("StockItem", item.item_code)
            if not result.get("exists"):
                missing.append({
                    "type": "Item",
                    "erpnext_doctype": "Item",
                    "name": item.item_code,
                    "display_name": item.item_name or item.item_code,
                    "parent": get_item_stock_group(item.item_code, company),
                    "priority": "Normal"
                })
        else:
            frappe.log_error(
                f"Item '{item.item_code}' not found in ERPNext. Cannot create Tally request.",
                "Dependency Checker - Item Not Found"
            )
    
    return missing

def check_purchase_invoice_dependencies(docname, doctype, company):
    """Check dependencies for Purchase Invoice/Order"""
    doc = frappe.get_doc(doctype, docname)
    missing = []
    
    # Check supplier
    supplier_exists_erpnext = frappe.db.exists("Supplier", doc.supplier)
    
    if supplier_exists_erpnext:
        result = check_master_exists("Ledger", doc.supplier)
        if not result.get("exists"):
            missing.append({
                "type": "Supplier",
                "erpnext_doctype": "Supplier",
                "name": doc.supplier,
                "display_name": doc.supplier_name or doc.supplier,
                "parent": "Sundry Creditors",
                "priority": "High"
            })
    
    # Check items
    for item in doc.items:
        item_exists_erpnext = frappe.db.exists("Item", item.item_code)
        
        if item_exists_erpnext:
            result = check_master_exists("StockItem", item.item_code)
            if not result.get("exists"):
                missing.append({
                    "type": "Item",
                    "erpnext_doctype": "Item",
                    "name": item.item_code,
                    "display_name": item.item_name or item.item_code,
                    "parent": get_item_stock_group(item.item_code, company),
                    "priority": "Normal"
                })
    
    return missing

def get_customer_parent_group(customer_name, company):
    """Get parent group for customer"""
    try:
        from tally_connect.tally_integration.utils import get_settings
        
        customer = frappe.get_doc("Customer", customer_name)
        for account in customer.accounts:
            if account.company == company and account.account:
                parent_account = frappe.db.get_value("Account", account.account, "parent_account")
                if parent_account:
                    return frappe.db.get_value("Account", parent_account, "account_name")
        
        settings = get_settings()
        return settings.get("default_customer_ledger") or "Sundry Debtors"
    except:
        return "Sundry Debtors"

def get_item_stock_group(item_code, company):
    """Get stock group for item"""
    try:
        from tally_connect.tally_integration.utils import get_settings
        
        item = frappe.get_doc("Item", item_code)
        group_mapping = {
            "Raw Material": "Raw Materials",
            "Finished Goods": "Finished Products",
            "Consumables": "Consumables",
            "Services": "Services"
        }
        
        if item.item_group in group_mapping:
            return group_mapping[item.item_group]
        
        settings = get_settings()
        return settings.get("default_inventory_stock_group") or "Primary"
    except:
        return "Primary"

@frappe.whitelist()
def check_dependencies_and_show_missing(doctype, docname, company):
    """API endpoint for UI button"""
    missing = check_dependencies_for_document(doctype, docname, company)
    
    return {
        "has_missing": len(missing) > 0,
        "missing_count": len(missing),
        "missing_masters": missing
    }

@frappe.whitelist()
def create_requests_for_missing_masters(doctype, docname, company, missing_masters_json):
    """Create requests for missing masters - WITH ERROR HANDLING"""
    import json
    
    missing_masters = json.loads(missing_masters_json)
    requests_created = []
    errors = []
    
    for master in missing_masters:
        # Check if request already exists
        existing = frappe.db.exists(
            "Tally Master Creation Request",
            {
                "erpnext_document": master["name"],
                "status": ["in", ["Pending Approval", "Approved", "In Progress"]]
            }
        )
        
        if existing:
            requests_created.append(existing)
            continue
        
        # Create new request
        try:
            # Truncate long names (max 140 chars for Title field)
            master_name_display = master["display_name"]
            if len(master_name_display) > 137:
                master_name_display = master_name_display[:137] + "..."
            
            request = frappe.get_doc({
                "doctype": "Tally Master Creation Request",
                "master_type": master["type"],
                "erpnext_doctype": master.get("erpnext_doctype"),
                "erpnext_document": master["name"],
                "master_name": master_name_display,
                "parent_group": master.get("parent", ""),
                "company": company,
                "linked_transaction": docname,
                "linked_transaction_doctype": doctype,
                "priority": master.get("priority", "Normal"),
                "status": "Pending Approval",
                "requested_by": frappe.session.user,
                "reason_for_creation": f"Required for {doctype}: {docname}"
            })
            request.insert(ignore_permissions=True)
            requests_created.append(request.name)
            
        except Exception as e:
            error_msg = str(e)
            frappe.log_error(
                f"Failed to create request for {master.get('display_name', 'Unknown')}:\n{error_msg}",
                "Create Tally Request Failed"
            )
            errors.append(f"{master.get('display_name', 'Unknown')[:50]} - {error_msg[:100]}")
    
    frappe.db.commit()
    
    # Build message
    if requests_created:
        msg = f"✅ Created {len(requests_created)} request(s)"
        if errors:
            msg += f"\n\n⚠️ {len(errors)} failed:\n" + "\n".join(errors[:2])
    else:
        msg = "❌ No requests created. See Error Log for details."
    
    return {
        "success": len(requests_created) > 0,
        "requests_created": requests_created,
        "message": msg,
        "errors": errors
    }
