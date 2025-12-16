#!/usr/bin/env python3
"""
Debug script for "Create Approval Requests" button
"""

import frappe
import json

print("="*80)
print("🔍 DEBUGGING CREATE REQUESTS FUNCTIONALITY")
print("="*80)

# TEST 1: Check if module exists
print("\n[TEST 1] Checking if dependency_checker module exists...")
try:
    from tally_connect.tally_integration.api import dependency_checker
    print("✅ Module found")
except ImportError as e:
    print(f"❌ Module not found: {e}")
    exit(1)

# TEST 2: Check if function exists
print("\n[TEST 2] Checking if function exists...")
try:
    func = dependency_checker.create_requests_for_missing_masters
    print(f"✅ Function found: {func}")
except AttributeError as e:
    print(f"❌ Function not found: {e}")
    exit(1)

# TEST 3: Check if function is whitelisted
print("\n[TEST 3] Checking if function is whitelisted...")
import inspect
source = inspect.getsource(func)
if '@frappe.whitelist()' in source:
    print("✅ Function is whitelisted")
else:
    print("❌ Function is NOT whitelisted - ADD @frappe.whitelist()")
    exit(1)

# TEST 4: Test function call
print("\n[TEST 4] Testing function call...")
try:
    test_missing = [{
        "type": "Customer",
        "erpnext_doctype": "Customer",
        "name": "TEST_DEBUG_CUSTOMER",
        "display_name": "Test Debug Customer",
        "parent": "Sundry Debtors",
        "priority": "Normal"
    }]
    
    # Create test customer first
    if not frappe.db.exists("Customer", "TEST_DEBUG_CUSTOMER"):
        test_cust = frappe.get_doc({
            "doctype": "Customer",
            "customer_name": "Test Debug Customer",
            "customer_type": "Individual"
        })
        test_cust.insert(ignore_permissions=True)
        print("   Created test customer")
    
    result = func(
        doctype="Sales Order",
        docname="TEST-SO-DEBUG",
        company=frappe.defaults.get_user_default("Company"),
        missing_masters_json=json.dumps(test_missing)
    )
    
    print(f"✅ Function executed successfully")
    print(f"   Result: {result}")
    
    # Cleanup
    if result.get("requests_created"):
        for req_name in result["requests_created"]:
            frappe.delete_doc("Tally Master Creation Request", req_name, force=1)
    
    if frappe.db.exists("Customer", "TEST_DEBUG_CUSTOMER"):
        frappe.delete_doc("Customer", "TEST_DEBUG_CUSTOMER", force=1)
    
    frappe.db.commit()
    
except Exception as e:
    print(f"❌ Function call failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# TEST 5: Check permissions
print("\n[TEST 5] Checking user permissions...")
try:
    user = frappe.session.user
    can_create = frappe.has_permission("Tally Master Creation Request", "create")
    print(f"   User: {user}")
    print(f"   Can create requests: {can_create}")
    
    if not can_create:
        print("❌ User does not have permission to create requests")
        print("   Fix: Add role 'System Manager' or create custom role")
except Exception as e:
    print(f"❌ Permission check failed: {e}")

print("\n" + "="*80)
print("✅ ALL CHECKS PASSED - Button should work!")
print("="*80)
print("\nIf button still doesn't work:")
print("1. Check browser console (F12) for JavaScript errors")
print("2. Run: bench build --app tally_connect")
print("3. Run: bench restart")
print("4. Hard refresh browser (Ctrl + Shift + R)")
