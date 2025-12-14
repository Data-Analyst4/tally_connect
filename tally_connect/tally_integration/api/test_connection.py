# import frappe
# from tally_connect.tally_integration.utils import validate_tally_connection

# @frappe.whitelist()
# def test_connection():
#     """
#     Whitelist API to test Tally connection
#     Called from Tally Integration Settings button
#     """
#     result = validate_tally_connection()
    
#     if result["success"]:
#         frappe.msgprint(
#             msg=f"""<div style="font-size: 14px;">
#                 <p><strong style="color: green;">✓ Connection Successful</strong></p>
#                 <hr>
#                 <p><strong>Tally Version:</strong> {result.get('tally_version', 'Unknown')}</p>
#                 <p><strong>Active Company:</strong> {result.get('active_company', 'Unknown')}</p>
#                 <p><strong>Settings Enabled:</strong> Yes</p>
#                 <p><strong>Tally Reachable:</strong> Yes</p>
#                 <p><strong>Company Matches:</strong> Yes</p>
#             </div>""",
#             title="Tally Connection Test",
#             indicator="green"
#         )
#     else:
#         frappe.msgprint(
#             msg=f"""<div style="font-size: 14px;">
#                 <p><strong style="color: red;">✗ Connection Failed</strong></p>
#                 <hr>
#                 <p><strong>Error:</strong> {result['error']}</p>
#                 <p><strong>Settings Enabled:</strong> {result['checks'].get('settings_enabled', False)}</p>
#                 <p><strong>Tally Reachable:</strong> {result['checks'].get('tally_reachable', False)}</p>
#                 <p><strong>Company Matches:</strong> {result['checks'].get('company_matches', False)}</p>
#             </div>""",
#             title="Tally Connection Test Failed",
#             indicator="red"
#         )
    
#     return result
"""
Test and validation endpoints for Tally connection
Provides API endpoints for testing connectivity, masters, and configuration
"""

import frappe
from frappe import _


@frappe.whitelist()
def test_tally_connection():
    """
    COMPREHENSIVE TEST - Tests everything
    
    Usage from frontend:
        frappe.call({
            method: 'tally_connect.tally_integration.api.test_connection.test_tally_connection',
            callback: function(r) {
                console.log(r.message);
            }
        });
    
    Or from backend:
        from tally_connect.tally_integration.api.test_connection import test_tally_connection
        result = test_tally_connection()
    """
    
    # Import from parent module (utils.py)
    from tally_connect.tally_integration.utils import validate_tally_connection
    
    try:
        # Run comprehensive validation
        result = validate_tally_connection()
        
        # Format response for frontend
        return {
            "success": result.get("success", False),
            "message": result.get("message", ""),
            "tally_version": result.get("tally_version", "Unknown"),
            "active_company": result.get("active_company", "Unknown"),
            "warnings": result.get("warnings", []),
            "details": {
                "settings_enabled": result["checks"]["settings_enabled"],
                "tally_reachable": result["checks"]["tally_reachable"],
                "xml_processing": result["checks"]["xml_processing"],
                "company_verified": result["checks"]["company_verified"],
                "masters_validated": result["checks"]["masters_validated"],
            },
            "full_details": result.get("checks", {})
        }
    
    except Exception as e:
        frappe.log_error(f"Test connection failed: {str(e)}", "Tally Test")
        return {
            "success": False,
            "error": str(e),
            "message": "Connection test failed with exception"
        }


@frappe.whitelist()
def test_tally_url():
    """
    TEST 1: Tally URL & Connection
    
    Tests:
    - Is Tally HTTP server running?
    - Can we reach the URL?
    - Which version of Tally?
    """
    
    from tally_connect.tally_integration.utils import (
        is_enabled,
        get_tally_url,
        check_tally_connectivity
    )
    
    try:
        if not is_enabled():
            return {
                "success": False,
                "error": "Tally integration is disabled",
                "fix": "Enable in Tally Integration Settings"
            }
        
        url = get_tally_url()
        result = check_tally_connectivity(url)
        
        return {
            "test": "Tally URL & Connection",
            "url": url,
            "success": result.get("success", False),
            "version": result.get("version", "Unknown"),
            "message": result.get("message") or result.get("error"),
            "details": result
        }
    
    except Exception as e:
        return {
            "test": "Tally URL & Connection",
            "success": False,
            "error": str(e)
        }


@frappe.whitelist()
def test_company_match():
    """
    TEST 2: Company Match
    
    Tests:
    - Is correct company loaded in Tally?
    - Does it match settings?
    """
    
    from tally_connect.tally_integration.utils import (
        get_settings,
        verify_tally_company
    )
    
    try:
        settings = get_settings()
        
        if not settings.tally_company_name:
            return {
                "test": "Company Match",
                "success": False,
                "error": "Company name not configured in settings",
                "configured_company": None,
                "active_company": None,
                "matches": False
            }
        
        result = verify_tally_company(settings.tally_company_name, settings.tally_url)
        
        return {
            "test": "Company Match",
            "success": result.get("success", False),
            "configured_company": result.get("configured_company", "Not set"),
            "active_company": result.get("active_company", "Unknown"),
            "matches": result.get("matches", False),
            "message": result.get("message", ""),
            "warning": result.get("warning"),
            "details": result
        }
    
    except Exception as e:
        return {
            "test": "Company Match",
            "success": False,
            "error": str(e)
        }


@frappe.whitelist()
def test_all_masters():
    """
    TEST 3: All Masters Validation
    
    Tests:
    - Do all configured masters exist in Tally?
    - Lists missing masters
    - Lists existing masters
    """
    
    from tally_connect.tally_integration.utils import validate_required_masters
    
    try:
        result = validate_required_masters()
        
        return {
            "test": "All Masters Validation",
            "success": result.get("all_exist", False),
            "checked_count": result.get("checked_count", 0),
            "missing_count": len(result.get("missing_masters", [])),
            "existing_count": len(result.get("existing_masters", [])),
            "missing_masters": result.get("missing_masters", []),
            "existing_masters": result.get("existing_masters", []),
            "message": "All masters exist" if result.get("all_exist") else f"{len(result.get('missing_masters', []))} masters missing",
            "details": result
        }
    
    except Exception as e:
        return {
            "test": "All Masters Validation",
            "success": False,
            "error": str(e)
        }


@frappe.whitelist()
def test_specific_master(master_type, master_name):
    """
    TEST 4: Single Master Check
    
    Args:
        master_type: "Group", "Ledger", "StockGroup", "StockItem", "Godown"
        master_name: Name to check
    """
    
    from tally_connect.tally_integration.utils import check_master_exists
    
    try:
        result = check_master_exists(master_type, master_name)
        
        return {
            "test": f"Check {master_type}: {master_name}",
            "success": result.get("success", False),
            "exists": result.get("exists", False),
            "master_type": master_type,
            "master_name": master_name,
            "message": f"{master_type} '{master_name}' {'exists' if result.get('exists') else 'does not exist'} in Tally",
            "details": result
        }
    
    except Exception as e:
        return {
            "test": f"Check {master_type}: {master_name}",
            "success": False,
            "error": str(e)
        }


@frappe.whitelist()
def test_all_groups():
    """
    TEST 5: All Groups Validation
    
    Tests only GROUP masters from settings:
    - default_customer_ledger (actually a group)
    - default_supplier_ledger (actually a group)
    """
    
    from tally_connect.tally_integration.utils import (
        get_settings,
        check_master_exists
    )
    
    try:
        settings = get_settings()
        
        groups_to_check = []
        
        if settings.default_customer_ledger:
            groups_to_check.append({
                "name": settings.default_customer_ledger,
                "purpose": "Customer Parent Group"
            })
        
        if settings.default_supplier_ledger:
            groups_to_check.append({
                "name": settings.default_supplier_ledger,
                "purpose": "Supplier Parent Group"
            })
        
        if not groups_to_check:
            return {
                "test": "All Groups Validation",
                "success": False,
                "error": "No groups configured in settings",
                "groups_checked": 0
            }
        
        results = []
        all_exist = True
        
        for group in groups_to_check:
            check_result = check_master_exists("Group", group["name"])
            results.append({
                "name": group["name"],
                "purpose": group["purpose"],
                "exists": check_result.get("exists", False),
                "success": check_result.get("success", False)
            })
            
            if not check_result.get("exists"):
                all_exist = False
        
        missing_groups = [r for r in results if not r["exists"]]
        existing_groups = [r for r in results if r["exists"]]
        
        return {
            "test": "All Groups Validation",
            "success": all_exist,
            "groups_checked": len(groups_to_check),
            "missing_count": len(missing_groups),
            "existing_count": len(existing_groups),
            "missing_groups": missing_groups,
            "existing_groups": existing_groups,
            "message": "All groups exist" if all_exist else f"{len(missing_groups)} groups missing",
            "details": results
        }
    
    except Exception as e:
        return {
            "test": "All Groups Validation",
            "success": False,
            "error": str(e)
        }


@frappe.whitelist()
def test_xml_processing():
    """
    TEST 6: XML Processing
    
    Tests:
    - Can Tally process XML POST requests?
    - Critical for WSL environments
    """
    
    from tally_connect.tally_integration.utils import (
        get_tally_url,
        check_tally_xml_processing
    )
    
    try:
        url = get_tally_url()
        result = check_tally_xml_processing(url)
        
        return {
            "test": "XML Processing",
            "success": result.get("success", False),
            "message": result.get("message") or result.get("error"),
            "details": result
        }
    
    except Exception as e:
        return {
            "test": "XML Processing",
            "success": False,
            "error": str(e)
        }


@frappe.whitelist()
def get_validation_summary():
    """
    SUMMARY: Get formatted validation report
    
    Returns a user-friendly summary of all checks
    """
    
    from tally_connect.tally_integration.utils import validate_tally_connection
    
    try:
        result = validate_tally_connection()
        
        summary = {
            "overall_status": "✅ READY" if result["success"] and not result["warnings"] else "⚠️ READY WITH WARNINGS" if result["success"] else "❌ NOT READY",
            "checks": [],
            "warnings": result.get("warnings", []),
            "recommendations": []
        }
        
        # Check 1: Integration Enabled
        summary["checks"].append({
            "name": "Integration Enabled",
            "status": "✅ Enabled" if result["checks"]["settings_enabled"] else "❌ Disabled",
            "passed": result["checks"]["settings_enabled"]
        })
        
        # Check 2: Tally Reachable
        summary["checks"].append({
            "name": "Tally Connection",
            "status": f"✅ Connected ({result.get('tally_version', 'Unknown')})" if result["checks"]["tally_reachable"] else "❌ Not reachable",
            "passed": result["checks"]["tally_reachable"]
        })
        
        # Check 3: XML Processing
        summary["checks"].append({
            "name": "XML Processing",
            "status": "✅ Working" if result["checks"]["xml_processing"] else "❌ Failed",
            "passed": result["checks"]["xml_processing"]
        })
        
        # Check 4: Company Match
        company_status = "✅ Matched" if result["checks"]["company_verified"] else "⚠️ Mismatch"
        summary["checks"].append({
            "name": "Company Verification",
            "status": f"{company_status} ({result.get('active_company', 'Unknown')})",
            "passed": result["checks"]["company_verified"]
        })
        
        # Check 5: Masters
        masters_detail = result["checks"]["details"].get("masters", {})
        missing_count = len(masters_detail.get("missing_masters", []))
        masters_status = "✅ All present" if result["checks"]["masters_validated"] else f"⚠️ {missing_count} missing"
        
        summary["checks"].append({
            "name": "Required Masters",
            "status": masters_status,
            "passed": result["checks"]["masters_validated"],
            "missing": masters_detail.get("missing_masters", [])
        })
        
        # Add recommendations
        if not result["checks"]["masters_validated"]:
            summary["recommendations"].append("Create missing masters in Tally before syncing data")
        
        if not result["checks"]["company_verified"]:
            summary["recommendations"].append("Load the correct company in Tally or update settings")
        
        return summary
    
    except Exception as e:
        return {
            "overall_status": "❌ ERROR",
            "error": str(e)
        }


@frappe.whitelist()
def validate_gstin_api(gstin):
    """
    TEST 7: GSTIN Validation
    
    Args:
        gstin: GSTIN number to validate
    """
    
    from tally_connect.tally_integration.utils import validate_gstin
    
    try:
        result = validate_gstin(gstin)
        
        return {
            "test": "GSTIN Validation",
            "success": result.get("valid", False),
            "gstin": result.get("gstin", ""),
            "message": result.get("message") or result.get("error"),
            "details": result
        }
    
    except Exception as e:
        return {
            "test": "GSTIN Validation",
            "success": False,
            "error": str(e)
        }
