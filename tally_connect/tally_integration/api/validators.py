# """
# Tally Validator APIs
# Combined validation functions
# """

# import frappe
# from tally_connect.tally_integration.api.checkers import (
#     check_ledger_exists,
#     check_group_exists,
#     check_stock_item_exists
# )
# from tally_connect.tally_integration.utils import get_settings


# @frappe.whitelist()
# def validate_customer_for_tally(customer_name):
#     """
#     Validate if customer is ready to sync
    
#     Returns:
#         dict: {
#             "valid": bool,
#             "errors": [],
#             "warnings": []
#         }
#     """
#     settings = get_settings()
#     errors = []
#     warnings = []
    
#     # Check parent group exists
#     parent_group = settings.default_customer_ledger or "Sundry Debtors"
#     group_check = check_group_exists(parent_group)
    
#     if not group_check["exists"]:
#         errors.append(f"Parent group '{parent_group}' not found in Tally")
    
#     # Check if customer already exists
#     ledger_check = check_ledger_exists(customer_name)
#     if ledger_check["exists"]:
#         warnings.append(f"Customer already exists in Tally")
    
#     return {
#         "valid": len(errors) == 0,
#         "errors": errors,
#         "warnings": warnings
#     }


# @frappe.whitelist()
# def validate_item_for_tally(item_code):
#     """Validate if item is ready to sync"""
#     settings = get_settings()
#     errors = []
#     warnings = []
    
#     # Check stock group
#     stock_group = settings.default_inventory_stock_group or "Primary"
#     group_check = check_group_exists(stock_group)
    
#     if not group_check["exists"]:
#         errors.append(f"Stock group '{stock_group}' not found in Tally")
    
#     return {
#         "valid": len(errors) == 0,
#         "errors": errors,
#         "warnings": warnings
#     }


# @frappe.whitelist()
# def validate_invoice_for_tally(invoice_name):
#     """Comprehensive validation for invoice"""
#     # TODO: Implement full invoice validation
#     return {
#         "valid": False,
#         "errors": ["Not implemented yet"],
#         "warnings": []
#     }
"""
Tally Validator APIs
Combined validation functions
"""

import frappe
from tally_connect.tally_integration.api.checkers import (
    check_ledger_exists,
    check_group_exists,
    check_stock_item_exists
)
from tally_connect.tally_integration.utils import get_settings


@frappe.whitelist()
def validate_customer_for_tally(customer_name):
    """
    Validate if customer is ready to sync
    
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
        warnings.append(f"Customer already exists in Tally")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


@frappe.whitelist()
def validate_item_for_tally(item_code):
    """
    Validate if item is ready to sync
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
        warnings.append(f"Item already exists in Tally")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }
