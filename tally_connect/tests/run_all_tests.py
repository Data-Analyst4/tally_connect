#!/usr/bin/env python3
# =============================================================================
# Master Test Runner - Runs all tests
# =============================================================================

import frappe
import sys

def run_all_tests():
    """Run all test suites"""
    
    print("\n" + "="*80)
    print("🧪 TALLY CONNECT - MASTER TEST RUNNER")
    print("="*80 + "\n")
    
    all_passed = True
    
    # Test 1: Dependency Checker
    print("Running Test Suite 1: Dependency Checker...")
    try:
        from tally_connect.tests.test_dependency_checker import TestDependencyChecker
        tester1 = TestDependencyChecker()
        tester1.run_all_tests()
        if tester1.failed > 0:
            all_passed = False
    except Exception as e:
        print(f"❌ Test Suite 1 Failed: {str(e)}")
        all_passed = False
    
    # Test 2: Approval Workflow
    print("\n" + "="*80)
    print("Running Test Suite 2: Approval Workflow...")
    try:
        from tally_connect.tests.test_approval import TestApprovalWorkflow
        tester2 = TestApprovalWorkflow()
        tester2.run_all_tests()
        if tester2.failed > 0:
            all_passed = False
    except Exception as e:
        print(f"❌ Test Suite 2 Failed: {str(e)}")
        all_passed = False
    
    # Print final summary
    print("\n" + "="*80)
    if all_passed:
        print("✅ ALL TEST SUITES PASSED")
    else:
        print("❌ SOME TESTS FAILED - CHECK LOGS")
    print("="*80 + "\n")
    
    return all_passed

if __name__ == "__main__":
    result = run_all_tests()
    sys.exit(0 if result else 1)
