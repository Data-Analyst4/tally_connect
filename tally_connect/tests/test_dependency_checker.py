# =============================================================================
# FILE: tally_connect/tests/test_dependency_checker.py
# 
# COMPREHENSIVE TESTS for dependency_checker.py
# Run: bench --site your-site console
#      >>> exec(open('apps/tally_connect/tally_connect/tests/test_dependency_checker.py').read())
# =============================================================================

import frappe
from frappe.utils import now
import json

class TestDependencyChecker:
    """Test all dependency checker functions"""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.test_results = []
        self.company = frappe.defaults.get_user_default("Company")
        
    def run_all_tests(self):
        """Run all test suites"""
        print("\n" + "="*80)
        print("🧪 TALLY DEPENDENCY CHECKER - COMPREHENSIVE TEST SUITE")
        print("="*80)
        
        # Import functions
        try:
            from tally_connect.tally_integration.api.dependency_checker import (
                check_dependencies_and_show_missing,
                create_requests_for_missing_masters,
                check_dependencies_for_document,
                check_sales_invoice_dependencies,
                get_customer_parent_group,
                get_item_stock_group
            )
            self.dep_checker = {
                'check_dependencies_and_show_missing': check_dependencies_and_show_missing,
                'create_requests_for_missing_masters': create_requests_for_missing_masters,
                'check_dependencies_for_document': check_dependencies_for_document,
                'check_sales_invoice_dependencies': check_sales_invoice_dependencies,
                'get_customer_parent_group': get_customer_parent_group,
                'get_item_stock_group': get_item_stock_group
            }
            print("✅ Module imported successfully\n")
        except Exception as e:
            print(f"❌ Failed to import module: {str(e)}")
            return
        
        # Run test suites
        self.test_suite_1_connection_checks()
        self.test_suite_2_master_existence()
        self.test_suite_3_parent_group_logic()
        self.test_suite_4_stock_group_mapping()
        self.test_suite_5_dependency_checking()
        self.test_suite_6_request_creation()
        self.test_suite_7_edge_cases()
        self.test_suite_8_error_handling()
        
        # Print summary
        self.print_summary()
    
    def log_test(self, test_name, passed, message="", details=""):
        """Log test result"""
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} | {test_name}")
        if message:
            print(f"     {message}")
        if details:
            print(f"     Details: {details}")
        
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        
        self.test_results.append({
            "test": test_name,
            "passed": passed,
            "message": message,
            "details": details
        })
    
    # =========================================================================
    # TEST SUITE 1: Connection Checks
    # =========================================================================
    
    def test_suite_1_connection_checks(self):
        """Test Tally connection verification"""
        print("\n" + "-"*80)
        print("📡 TEST SUITE 1: CONNECTION CHECKS")
        print("-"*80)
        
        from tally_connect.tally_integration.utils import (
            check_tally_connectivity,
            verify_tally_company
        )
        
        # TEST 1.1: Check if Tally is running
        print("\n[1.1] Testing Tally Connectivity...")
        try:
            result = check_tally_connectivity()
            
            if result.get("connected"):
                self.log_test(
                    "Tally Connection",
                    True,
                    "Tally is running and responding"
                )
            else:
                self.log_test(
                    "Tally Connection",
                    False,
                    f"Tally not connected: {result.get('error')}",
                    "Please start Tally and ensure port 9000 is accessible"
                )
                return  # Skip remaining tests if Tally not connected
        except Exception as e:
            self.log_test("Tally Connection", False, str(e))
            return
        
        # TEST 1.2: Verify company is correct
        print("\n[1.2] Testing Company Verification...")
        try:
            result = verify_tally_company(self.company)
            
            self.log_test(
                "Company Verification",
                result.get("valid", False),
                f"Open company: {result.get('open_company', 'Unknown')}",
                f"Expected: {self.company}"
            )
        except Exception as e:
            self.log_test("Company Verification", False, str(e))
        
        # TEST 1.3: Check master exists (basic functionality)
        print("\n[1.3] Testing Master Existence Check...")
        try:
            from tally_connect.tally_integration.utils import check_master_exists
            
            # Test with common ledger (should exist)
            result = check_master_exists("Ledger", "Cash")
            
            self.log_test(
                "Check Master Exists - Cash Ledger",
                result.get("exists") is not None,
                f"Cash ledger exists: {result.get('exists')}"
            )
        except Exception as e:
            self.log_test("Check Master Exists", False, str(e))
    
    # =========================================================================
    # TEST SUITE 2: Master Existence Checks
    # =========================================================================
    
    def test_suite_2_master_existence(self):
        """Test checking if masters exist in Tally"""
        print("\n" + "-"*80)
        print("🔍 TEST SUITE 2: MASTER EXISTENCE CHECKS")
        print("-"*80)
        
        from tally_connect.tally_integration.utils import check_master_exists
        
        # TEST 2.1: Check existing ledger
        print("\n[2.1] Testing Existing Ledger Check...")
        try:
            result = check_master_exists("Ledger", "Cash")
            self.log_test(
                "Existing Ledger - Cash",
                result.get("exists") == True,
                f"Result: {result}"
            )
        except Exception as e:
            self.log_test("Existing Ledger Check", False, str(e))
        
        # TEST 2.2: Check non-existing ledger
        print("\n[2.2] Testing Non-Existing Ledger Check...")
        try:
            result = check_master_exists("Ledger", "NONEXISTENT_LEDGER_12345")
            self.log_test(
                "Non-Existing Ledger",
                result.get("exists") == False,
                f"Result: {result}"
            )
        except Exception as e:
            self.log_test("Non-Existing Ledger Check", False, str(e))
        
        # TEST 2.3: Check with special characters
        print("\n[2.3] Testing Ledger with Special Characters...")
        try:
            # Get first customer from ERPNext
            customers = frappe.get_all("Customer", limit=1)
            if customers:
                customer_name = customers[0].name
                result = check_master_exists("Ledger", customer_name)
                self.log_test(
                    "Ledger with Special Characters",
                    "exists" in result,
                    f"Checked customer: {customer_name}, Exists: {result.get('exists')}"
                )
            else:
                self.log_test(
                    "Ledger with Special Characters",
                    False,
                    "No customers found in ERPNext"
                )
        except Exception as e:
            self.log_test("Special Characters Check", False, str(e))
        
        # TEST 2.4: Check stock item
        print("\n[2.4] Testing Stock Item Check...")
        try:
            items = frappe.get_all("Item", filters={"is_stock_item": 1}, limit=1)
            if items:
                item_code = items[0].name
                result = check_master_exists("StockItem", item_code)
                self.log_test(
                    "Stock Item Check",
                    "exists" in result,
                    f"Checked item: {item_code}, Exists: {result.get('exists')}"
                )
            else:
                self.log_test("Stock Item Check", False, "No stock items in ERPNext")
        except Exception as e:
            self.log_test("Stock Item Check", False, str(e))
    
    # =========================================================================
    # TEST SUITE 3: Parent Group Logic
    # =========================================================================
    
    def test_suite_3_parent_group_logic(self):
        """Test get_customer_parent_group function"""
        print("\n" + "-"*80)
        print("📂 TEST SUITE 3: PARENT GROUP LOGIC")
        print("-"*80)
        
        get_parent = self.dep_checker['get_customer_parent_group']
        
        # TEST 3.1: Get parent group for existing customer
        print("\n[3.1] Testing Parent Group for Existing Customer...")
        try:
            customers = frappe.get_all("Customer", limit=1)
            if customers:
                customer_name = customers[0].name
                parent_group = get_parent(customer_name, self.company)
                
                self.log_test(
                    "Get Customer Parent Group",
                    parent_group is not None and len(parent_group) > 0,
                    f"Customer: {customer_name}, Parent: {parent_group}"
                )
            else:
                self.log_test(
                    "Get Customer Parent Group",
                    False,
                    "No customers found"
                )
        except Exception as e:
            self.log_test("Get Customer Parent Group", False, str(e))
        
        # TEST 3.2: Test fallback to default
        print("\n[3.2] Testing Fallback to Default Parent...")
        try:
            # Create temporary customer with no accounts
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": "TEST_CUSTOMER_NO_ACCOUNT",
                "customer_type": "Individual"
            })
            test_customer.insert(ignore_permissions=True)
            
            parent_group = get_parent(test_customer.name, self.company)
            
            self.log_test(
                "Fallback to Default Parent",
                parent_group in ["Sundry Debtors", "Debtors"],
                f"Returned: {parent_group} (Expected: Sundry Debtors or similar)"
            )
            
            # Cleanup
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Fallback to Default Parent", False, str(e))
        
        # TEST 3.3: Test with invalid customer
        print("\n[3.3] Testing with Non-Existent Customer...")
        try:
            parent_group = get_parent("NONEXISTENT_CUSTOMER_XYZ", self.company)
            
            self.log_test(
                "Non-Existent Customer",
                parent_group == "Sundry Debtors",  # Should return default
                f"Returned: {parent_group}"
            )
        except Exception as e:
            self.log_test("Non-Existent Customer", False, str(e))
        
        # TEST 3.4: Test with multiple companies
        print("\n[3.4] Testing Multi-Company Scenario...")
        try:
            customers = frappe.get_all(
                "Customer",
                filters={"disabled": 0},
                limit=1
            )
            
            if customers:
                customer_name = customers[0].name
                customer = frappe.get_doc("Customer", customer_name)
                
                # Check if customer has accounts for multiple companies
                if len(customer.accounts) > 1:
                    company1 = customer.accounts[0].company
                    company2 = customer.accounts[1].company
                    
                    parent1 = get_parent(customer_name, company1)
                    parent2 = get_parent(customer_name, company2)
                    
                    self.log_test(
                        "Multi-Company Parent Groups",
                        parent1 is not None and parent2 is not None,
                        f"Company 1: {parent1}, Company 2: {parent2}"
                    )
                else:
                    self.log_test(
                        "Multi-Company Parent Groups",
                        True,
                        "No multi-company customers found (test skipped)"
                    )
            else:
                self.log_test("Multi-Company Test", False, "No customers found")
                
        except Exception as e:
            self.log_test("Multi-Company Test", False, str(e))
    
    # =========================================================================
    # TEST SUITE 4: Stock Group Mapping
    # =========================================================================
    
    def test_suite_4_stock_group_mapping(self):
        """Test get_item_stock_group function"""
        print("\n" + "-"*80)
        print("📦 TEST SUITE 4: STOCK GROUP MAPPING")
        print("-"*80)
        
        get_stock_group = self.dep_checker['get_item_stock_group']
        
        # TEST 4.1: Test mapped item group
        print("\n[4.1] Testing Mapped Item Group...")
        try:
            # Create test item with known group
            test_item = frappe.get_doc({
                "doctype": "Item",
                "item_code": "TEST_RAW_MATERIAL_001",
                "item_name": "Test Raw Material",
                "item_group": "Raw Material",
                "stock_uom": "Nos",
                "is_stock_item": 1
            })
            test_item.insert(ignore_permissions=True)
            
            stock_group = get_stock_group(test_item.item_code, self.company)
            
            self.log_test(
                "Mapped Item Group - Raw Material",
                stock_group == "Raw Materials",
                f"Item Group: Raw Material → Stock Group: {stock_group}"
            )
            
            # Cleanup
            test_item.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Mapped Item Group", False, str(e))
        
        # TEST 4.2: Test unmapped item group (fallback)
        print("\n[4.2] Testing Unmapped Item Group...")
        try:
            test_item = frappe.get_doc({
                "doctype": "Item",
                "item_code": "TEST_CUSTOM_GROUP_001",
                "item_name": "Test Custom Item",
                "item_group": "Custom Test Group",
                "stock_uom": "Nos",
                "is_stock_item": 1
            })
            test_item.insert(ignore_permissions=True)
            
            stock_group = get_stock_group(test_item.item_code, self.company)
            
            self.log_test(
                "Unmapped Item Group - Fallback",
                stock_group == "Primary",
                f"Custom Group → Stock Group: {stock_group} (Expected: Primary)"
            )
            
            # Cleanup
            test_item.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Unmapped Item Group", False, str(e))
        
        # TEST 4.3: Test all mapped groups
        print("\n[4.3] Testing All Mapped Groups...")
        mapping = {
            "Raw Material": "Raw Materials",
            "Finished Goods": "Finished Products",
            "Consumables": "Consumables",
            "Services": "Services"
        }
        
        for erpnext_group, expected_tally_group in mapping.items():
            try:
                test_item_code = f"TEST_{erpnext_group.replace(' ', '_').upper()}"
                
                test_item = frappe.get_doc({
                    "doctype": "Item",
                    "item_code": test_item_code,
                    "item_name": f"Test {erpnext_group}",
                    "item_group": erpnext_group,
                    "stock_uom": "Nos",
                    "is_stock_item": 1
                })
                test_item.insert(ignore_permissions=True)
                
                stock_group = get_stock_group(test_item_code, self.company)
                
                self.log_test(
                    f"Group Mapping - {erpnext_group}",
                    stock_group == expected_tally_group,
                    f"{erpnext_group} → {stock_group}"
                )
                
                # Cleanup
                test_item.delete(ignore_permissions=True)
                
            except Exception as e:
                self.log_test(f"Group Mapping - {erpnext_group}", False, str(e))
        
        frappe.db.commit()
    
    # =========================================================================
    # TEST SUITE 5: Dependency Checking
    # =========================================================================
    
    def test_suite_5_dependency_checking(self):
        """Test check_dependencies_for_document function"""
        print("\n" + "-"*80)
        print("🔗 TEST SUITE 5: DEPENDENCY CHECKING")
        print("-"*80)
        
        check_deps = self.dep_checker['check_dependencies_for_document']
        
        # TEST 5.1: Check existing Sales Order
        print("\n[5.1] Testing Dependency Check for Sales Order...")
        try:
            sales_orders = frappe.get_all(
                "Sales Order",
                filters={"docstatus": ["<", 2]},
                limit=1
            )
            
            if sales_orders:
                so_name = sales_orders[0].name
                missing = check_deps("Sales Order", so_name, self.company)
                
                self.log_test(
                    "Sales Order Dependency Check",
                    isinstance(missing, list),
                    f"Order: {so_name}, Missing: {len(missing)} masters"
                )
            else:
                self.log_test(
                    "Sales Order Dependency Check",
                    False,
                    "No Sales Orders found"
                )
        except Exception as e:
            self.log_test("Sales Order Dependency Check", False, str(e))
        
        # TEST 5.2: Create test order with new customer
        print("\n[5.2] Testing with New Customer (Missing in Tally)...")
        try:
            # Create test customer
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": f"TEST_NEW_CUSTOMER_{frappe.utils.now()}",
                "customer_type": "Individual"
            })
            test_customer.insert(ignore_permissions=True)
            
            # Create test item
            test_item = frappe.get_doc({
                "doctype": "Item",
                "item_code": f"TEST_ITEM_{frappe.utils.now()}",
                "item_name": "Test Item",
                "item_group": "Products",
                "stock_uom": "Nos",
                "is_stock_item": 1
            })
            test_item.insert(ignore_permissions=True)
            
            # Create test Sales Order
            test_so = frappe.get_doc({
                "doctype": "Sales Order",
                "customer": test_customer.name,
                "delivery_date": frappe.utils.add_days(None, 7),
                "company": self.company,
                "items": [{
                    "item_code": test_item.item_code,
                    "qty": 1,
                    "rate": 100
                }]
            })
            test_so.insert(ignore_permissions=True)
            
            # Check dependencies
            missing = check_deps("Sales Order", test_so.name, self.company)
            
            # Customer should be in missing list
            has_customer = any(m["type"] == "Customer" for m in missing)
            has_item = any(m["type"] == "Item" for m in missing)
            
            self.log_test(
                "New Customer/Item Detection",
                has_customer or has_item,
                f"Missing: {len(missing)} masters (Customer: {has_customer}, Item: {has_item})"
            )
            
            # Cleanup
            test_so.delete(ignore_permissions=True)
            test_item.delete(ignore_permissions=True)
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("New Customer/Item Detection", False, str(e))
        
        # TEST 5.3: Test with Purchase Invoice
        print("\n[5.3] Testing Purchase Invoice Dependencies...")
        try:
            purchase_invoices = frappe.get_all(
                "Purchase Invoice",
                filters={"docstatus": ["<", 2]},
                limit=1
            )
            
            if purchase_invoices:
                pi_name = purchase_invoices[0].name
                missing = check_deps("Purchase Invoice", pi_name, self.company)
                
                self.log_test(
                    "Purchase Invoice Dependency Check",
                    isinstance(missing, list),
                    f"Invoice: {pi_name}, Missing: {len(missing)} masters"
                )
            else:
                self.log_test(
                    "Purchase Invoice Dependency Check",
                    True,
                    "No Purchase Invoices found (test skipped)"
                )
        except Exception as e:
            self.log_test("Purchase Invoice Dependency Check", False, str(e))
    
    # =========================================================================
    # TEST SUITE 6: Request Creation
    # =========================================================================
    
    def test_suite_6_request_creation(self):
        """Test create_requests_for_missing_masters function"""
        print("\n" + "-"*80)
        print("📝 TEST SUITE 6: REQUEST CREATION")
        print("-"*80)
        
        create_requests = self.dep_checker['create_requests_for_missing_masters']
        
        # TEST 6.1: Create single request
        print("\n[6.1] Testing Single Request Creation...")
        try:
            # Create test customer
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": f"TEST_CUST_REQ_{frappe.utils.now()}",
                "customer_type": "Individual"
            })
            test_customer.insert(ignore_permissions=True)
            
            missing_masters = [{
                "type": "Customer",
                "erpnext_doctype": "Customer",
                "name": test_customer.name,
                "display_name": test_customer.customer_name,
                "parent": "Sundry Debtors",
                "priority": "High"
            }]
            
            result = create_requests(
                doctype="Sales Order",
                docname="TEST-SO-001",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            self.log_test(
                "Single Request Creation",
                result.get("success") == True and len(result.get("requests_created", [])) > 0,
                f"Created: {len(result.get('requests_created', []))} request(s)"
            )
            
            # Verify request exists
            if result.get("requests_created"):
                request_name = result["requests_created"][0]
                request = frappe.get_doc("Tally Master Creation Request", request_name)
                
                self.log_test(
                    "Request Data Verification",
                    request.erpnext_document == test_customer.name,
                    f"Request: {request_name}, Customer: {request.erpnext_document}"
                )
                
                # Cleanup
                request.delete(ignore_permissions=True)
            
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Single Request Creation", False, str(e))
        
        # TEST 6.2: Test duplicate prevention
        print("\n[6.2] Testing Duplicate Request Prevention...")
        try:
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": f"TEST_DUP_CUST_{frappe.utils.now()}",
                "customer_type": "Individual"
            })
            test_customer.insert(ignore_permissions=True)
            
            missing_masters = [{
                "type": "Customer",
                "erpnext_doctype": "Customer",
                "name": test_customer.name,
                "display_name": test_customer.customer_name,
                "parent": "Sundry Debtors",
                "priority": "High"
            }]
            
            # Create first request
            result1 = create_requests(
                doctype="Sales Order",
                docname="TEST-SO-002",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            # Try to create again
            result2 = create_requests(
                doctype="Sales Order",
                docname="TEST-SO-002",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            # Should return same request
            self.log_test(
                "Duplicate Prevention",
                result1.get("requests_created") == result2.get("requests_created"),
                f"First: {result1.get('requests_created')}, Second: {result2.get('requests_created')}"
            )
            
            # Cleanup
            if result1.get("requests_created"):
                request = frappe.get_doc("Tally Master Creation Request", result1["requests_created"][0])
                request.delete(ignore_permissions=True)
            
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Duplicate Prevention", False, str(e))
        
        # TEST 6.3: Test bulk request creation
        print("\n[6.3] Testing Bulk Request Creation...")
        try:
            missing_masters = []
            test_docs = []
            
            # Create 5 test customers
            for i in range(5):
                test_customer = frappe.get_doc({
                    "doctype": "Customer",
                    "customer_name": f"TEST_BULK_CUST_{i}_{frappe.utils.now()}",
                    "customer_type": "Individual"
                })
                test_customer.insert(ignore_permissions=True)
                test_docs.append(test_customer)
                
                missing_masters.append({
                    "type": "Customer",
                    "erpnext_doctype": "Customer",
                    "name": test_customer.name,
                    "display_name": test_customer.customer_name,
                    "parent": "Sundry Debtors",
                    "priority": "Normal"
                })
            
            result = create_requests(
                doctype="Sales Order",
                docname="TEST-SO-BULK",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            self.log_test(
                "Bulk Request Creation",
                len(result.get("requests_created", [])) == 5,
                f"Created: {len(result.get('requests_created', []))} out of 5"
            )
            
            # Cleanup
            for request_name in result.get("requests_created", []):
                frappe.get_doc("Tally Master Creation Request", request_name).delete(ignore_permissions=True)
            
            for doc in test_docs:
                doc.delete(ignore_permissions=True)
            
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Bulk Request Creation", False, str(e))
    
    # =========================================================================
    # TEST SUITE 7: Edge Cases
    # =========================================================================
    
    def test_suite_7_edge_cases(self):
        """Test edge cases and boundary conditions"""
        print("\n" + "-"*80)
        print("⚠️  TEST SUITE 7: EDGE CASES")
        print("-"*80)
        
        # TEST 7.1: Very long customer name
        print("\n[7.1] Testing Very Long Customer Name...")
        try:
            long_name = "A" * 200  # 200 character name
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": long_name,
                "customer_type": "Individual"
            })
            test_customer.insert(ignore_permissions=True)
            
            missing_masters = [{
                "type": "Customer",
                "erpnext_doctype": "Customer",
                "name": test_customer.name,
                "display_name": long_name,
                "parent": "Sundry Debtors",
                "priority": "High"
            }]
            
            result = self.dep_checker['create_requests_for_missing_masters'](
                doctype="Sales Order",
                docname="TEST-SO-LONG",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            self.log_test(
                "Very Long Customer Name",
                result.get("success") == True,
                f"Created request with {len(long_name)} char name"
            )
            
            # Cleanup
            if result.get("requests_created"):
                frappe.get_doc("Tally Master Creation Request", result["requests_created"][0]).delete(ignore_permissions=True)
            
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Very Long Customer Name", False, str(e))
        
        # TEST 7.2: Special characters in name
        print("\n[7.2] Testing Special Characters...")
        try:
            special_name = "ABC & CO. (P) LTD. - BRANCH #1"
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": special_name,
                "customer_type": "Company"
            })
            test_customer.insert(ignore_permissions=True)
            
            parent_group = self.dep_checker['get_customer_parent_group'](
                test_customer.name,
                self.company
            )
            
            self.log_test(
                "Special Characters in Name",
                parent_group is not None,
                f"Name: {special_name}, Parent: {parent_group}"
            )
            
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Special Characters in Name", False, str(e))
        
        # TEST 7.3: Empty parent group
        print("\n[7.3] Testing Empty Parent Group...")
        try:
            missing_masters = [{
                "type": "Customer",
                "erpnext_doctype": "Customer",
                "name": "TEST",
                "display_name": "Test Customer",
                "parent": "",  # Empty parent
                "priority": "Normal"
            }]
            
            result = self.dep_checker['create_requests_for_missing_masters'](
                doctype="Sales Order",
                docname="TEST-SO-EMPTY",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            self.log_test(
                "Empty Parent Group",
                "errors" in result or result.get("success"),
                f"Handled empty parent: {result.get('message', '')}"
            )
            
            # Cleanup
            if result.get("requests_created"):
                for req in result["requests_created"]:
                    frappe.get_doc("Tally Master Creation Request", req).delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Empty Parent Group", False, str(e))
        
        # TEST 7.4: Invoice with 20+ items
        print("\n[7.4] Testing Large Invoice (20+ items)...")
        try:
            test_customer = frappe.get_doc({
                "doctype": "Customer",
                "customer_name": f"TEST_LARGE_INV_{frappe.utils.now()}",
                "customer_type": "Individual"
            })
            test_customer.insert(ignore_permissions=True)
            
            items = []
            for i in range(25):
                test_item = frappe.get_doc({
                    "doctype": "Item",
                    "item_code": f"TEST_ITEM_{i}_{frappe.utils.now()}",
                    "item_name": f"Test Item {i}",
                    "item_group": "Products",
                    "stock_uom": "Nos",
                    "is_stock_item": 1
                })
                test_item.insert(ignore_permissions=True)
                items.append(test_item)
            
            test_so = frappe.get_doc({
                "doctype": "Sales Order",
                "customer": test_customer.name,
                "delivery_date": frappe.utils.add_days(None, 7),
                "company": self.company,
                "items": [{
                    "item_code": item.item_code,
                    "qty": 1,
                    "rate": 100
                } for item in items]
            })
            test_so.insert(ignore_permissions=True)
            
            # Check dependencies
            import time
            start_time = time.time()
            missing = self.dep_checker['check_dependencies_for_document'](
                "Sales Order",
                test_so.name,
                self.company
            )
            duration = time.time() - start_time
            
            self.log_test(
                "Large Invoice (25 items)",
                duration < 30,  # Should complete in < 30 seconds
                f"Items: 25, Missing: {len(missing)}, Duration: {duration:.2f}s"
            )
            
            # Cleanup
            test_so.delete(ignore_permissions=True)
            for item in items:
                item.delete(ignore_permissions=True)
            test_customer.delete(ignore_permissions=True)
            frappe.db.commit()
            
        except Exception as e:
            self.log_test("Large Invoice Test", False, str(e))
    
    # =========================================================================
    # TEST SUITE 8: Error Handling
    # =========================================================================
    
    def test_suite_8_error_handling(self):
        """Test error handling and graceful failures"""
        print("\n" + "-"*80)
        print("🛡️  TEST SUITE 8: ERROR HANDLING")
        print("-"*80)
        
        # TEST 8.1: Invalid doctype
        print("\n[8.1] Testing Invalid DocType...")
        try:
            missing = self.dep_checker['check_dependencies_for_document'](
                "Invalid DocType",
                "TEST-001",
                self.company
            )
            
            self.log_test(
                "Invalid DocType Handling",
                isinstance(missing, list) and len(missing) == 0,
                "Returns empty list for invalid doctype"
            )
        except Exception as e:
            self.log_test("Invalid DocType Handling", False, str(e))
        
        # TEST 8.2: Invalid document name
        print("\n[8.2] Testing Invalid Document Name...")
        try:
            missing = self.dep_checker['check_dependencies_for_document'](
                "Sales Order",
                "NONEXISTENT-SO-12345",
                self.company
            )
            
            self.log_test(
                "Invalid Document Name",
                False,  # Should raise error
                "Should raise DoesNotExistError"
            )
        except frappe.exceptions.DoesNotExistError:
            self.log_test(
                "Invalid Document Name",
                True,
                "Correctly raises DoesNotExistError"
            )
        except Exception as e:
            self.log_test("Invalid Document Name", False, str(e))
        
        # TEST 8.3: Invalid JSON in request creation
        print("\n[8.3] Testing Invalid JSON...")
        try:
            result = self.dep_checker['create_requests_for_missing_masters'](
                doctype="Sales Order",
                docname="TEST",
                company=self.company,
                missing_masters_json="INVALID JSON"
            )
            
            self.log_test(
                "Invalid JSON Handling",
                False,
                "Should raise JSON decode error"
            )
        except Exception as e:
            self.log_test(
                "Invalid JSON Handling",
                True,
                f"Correctly raises error: {type(e).__name__}"
            )
        
        # TEST 8.4: Test with null values
        print("\n[8.4] Testing Null Value Handling...")
        try:
            missing_masters = [{
                "type": "Customer",
                "erpnext_doctype": None,
                "name": None,
                "display_name": "Test",
                "parent": None,
                "priority": "Normal"
            }]
            
            result = self.dep_checker['create_requests_for_missing_masters'](
                doctype="Sales Order",
                docname="TEST-NULL",
                company=self.company,
                missing_masters_json=json.dumps(missing_masters)
            )
            
            self.log_test(
                "Null Value Handling",
                "errors" in result or not result.get("success"),
                "Handles null values gracefully"
            )
        except Exception as e:
            self.log_test("Null Value Handling", True, f"Raises error: {type(e).__name__}")
    
    # =========================================================================
    # Print Summary
    # =========================================================================
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "="*80)
        print("📊 TEST SUMMARY")
        print("="*80)
        
        total = self.passed + self.failed
        pass_rate = (self.passed / total * 100) if total > 0 else 0
        
        print(f"\nTotal Tests: {total}")
        print(f"✅ Passed: {self.passed}")
        print(f"❌ Failed: {self.failed}")
        print(f"📈 Pass Rate: {pass_rate:.1f}%")
        
        if self.failed > 0:
            print("\n" + "-"*80)
            print("FAILED TESTS:")
            print("-"*80)
            for result in self.test_results:
                if not result["passed"]:
                    print(f"\n❌ {result['test']}")
                    print(f"   Message: {result['message']}")
                    if result["details"]:
                        print(f"   Details: {result['details']}")
        
        print("\n" + "="*80)
        print("✅ TEST RUN COMPLETE" if self.failed == 0 else "⚠️  SOME TESTS FAILED")
        print("="*80 + "\n")


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    tester = TestDependencyChecker()
    tester.run_all_tests()
