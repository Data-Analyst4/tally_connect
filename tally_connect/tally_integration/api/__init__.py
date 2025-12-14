"""
Tally Integration API

Public APIs for Tally operations
Generated from actual function scan - December 14, 2025

All imports verified to exist in source files
"""

# ============================================================================
# CREATORS - 5 Whitelisted Public APIs
# ============================================================================
from tally_connect.tally_integration.api.creators import (
    create_group_in_tally,              # ✅ Whitelisted
    create_customer_ledger_in_tally,    # ✅ Whitelisted
    create_supplier_ledger_in_tally,    # ✅ Whitelisted
    create_stock_group_in_tally,        # ✅ Whitelisted
    create_stock_item_in_tally          # ✅ Whitelisted
)

# ============================================================================
# CHECKERS - 11 Whitelisted Public APIs
# ============================================================================
from tally_connect.tally_integration.api.checkers import (
    check_ledger_exists,                # ✅ Whitelisted
    check_group_exists,                 # ✅ Whitelisted
    check_stock_item_exists,            # ✅ Whitelisted
    check_stock_group_exists,           # ✅ Whitelisted
    check_godown_exists,                # ✅ Whitelisted
    check_unit_exists,                  # ✅ Whitelisted
    check_gst_classification_exists,    # ✅ Whitelisted
    batch_check_masters,                # ✅ Whitelisted
    check_document_dependencies,        # ✅ Whitelisted
    check_voucher_exists,               # ✅ Whitelisted
    check_tally_company                 # ✅ Whitelisted
)

# ============================================================================
# VALIDATORS - 2 Whitelisted Public APIs
# ============================================================================
from tally_connect.tally_integration.api.validators import (
    validate_customer_for_tally,        # ✅ Whitelisted
    validate_item_for_tally             # ✅ Whitelisted
)

# ============================================================================
# PUBLIC API EXPORTS
# ============================================================================
__all__ = [
    # Creators (5)
    'create_group_in_tally',
    'create_customer_ledger_in_tally',
    'create_supplier_ledger_in_tally',
    'create_stock_group_in_tally',
    'create_stock_item_in_tally',
    
    # Checkers (11)
    'check_ledger_exists',
    'check_group_exists',
    'check_stock_item_exists',
    'check_stock_group_exists',
    'check_godown_exists',
    'check_unit_exists',
    'check_gst_classification_exists',
    'batch_check_masters',
    'check_document_dependencies',
    'check_voucher_exists',
    'check_tally_company',
    
    # Validators (2)
    'validate_customer_for_tally',
    'validate_item_for_tally'
]

# Total: 18 public APIs available
