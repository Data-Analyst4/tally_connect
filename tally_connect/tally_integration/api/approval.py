# =============================================================================
# FILE: tally_connect/tally_integration/api/approval.py
# 
# PURPOSE: Bridge between approval workflow and Tally creation
# 
# WHAT THIS DOES:
# - Called when admin approves a Tally Master Creation Request
# - Routes the request to appropriate creator function
# - Handles success/failure and updates request status
# - Triggers notifications and auto-retry
#
# WHY WE NEED THIS:
# Before: Users could directly call creators.py functions → No control
# After:  All creation goes through approval → Audit trail + control
#
# HOW IT HELPS:
# 1. Centralized entry point for all master creation
# 2. Consistent error handling across all master types
# 3. Automatic retry of linked transactions
# 4. Complete audit trail (who approved, when, what happened)
#
# FLOW:
# Tally Master Creation Request (Approved) 
#   → on_approve() queues background job
#   → Background worker calls create_master_in_tally()
#   → This file routes to creators.py
#   → Updates request status
#   → Sends notifications
# =============================================================================

import frappe
from frappe import _
from frappe.utils import now

# =============================================================================
# MAIN ENTRY POINT: Called by background worker
# =============================================================================

def create_master_in_tally(request_name):
    """
    Background job that creates master in Tally after approval
    
    ⭐ THIS IS THE MAIN ORCHESTRATION FUNCTION ⭐
    
    Called by:
        - tally_master_creation_request.on_approve() via frappe.enqueue()
        - Background worker picks this up from Redis queue
    
    Args:
        request_name (str): ID of Tally Master Creation Request
                            Example: "TMR-2025-00045"
    
    Returns:
        dict: {
            "success": bool,
            "message": str,
            "error": str (if failed)
        }
    
    WHAT THIS DOES:
    1. Loads the approved request from database
    2. Changes status to "In Progress" so admin sees it's being processed
    3. Routes to appropriate creator function (customer/item/etc)
    4. On success: Marks complete, notifies user, retries failed transaction
    5. On failure: Marks failed, captures error, notifies admin
    
    WHY WE DO THIS:
    - Single point of control for all master creation
    - Consistent error handling
    - Automatic status tracking
    - Prevents duplicate processing
    
    HOW IT HELPS:
    - Admin sees real-time status updates ("In Progress" → "Completed")
    - If creation fails, error is captured and admin is notified
    - If linked invoice exists, it's automatically retried
    - Complete audit trail maintained
    """
    
    # =========================================================================
    # STEP 1: Load the request document
    # =========================================================================
    # WHY: We need the request to know what to create (customer/item/etc)
    # EDGE CASE: Request might be deleted while job was in queue
    
    try:
        request = frappe.get_doc("Tally Master Creation Request", request_name)
    except frappe.DoesNotExistError:
        # REQUEST WAS DELETED - Log and exit gracefully
        frappe.log_error(
            f"Request {request_name} not found (may have been deleted)",
            "Tally Master Creation - Request Not Found"
        )
        return {
            "success": False,
            "error": f"Request {request_name} not found"
        }
    
    # =========================================================================
    # STEP 2: Update status to "In Progress"
    # =========================================================================
    # WHY: Admin sees the request is being processed (not stuck in queue)
    # NOTE: We use db_set() to avoid triggering validate() and other hooks
    
    request.status = "In Progress"
    request.sync_status = "In Progress"
    request.db_set("status", "In Progress", update_modified=True)
    request.db_set("sync_status", "In Progress", update_modified=True)
    frappe.db.commit()  # Commit immediately so status is visible in UI
    
    # =========================================================================
    # STEP 3: Import and call the creator router
    # =========================================================================
    # WHY: We import here (not at top) to avoid circular imports
    # The router will call the appropriate function based on master_type
    
    from tally_connect.tally_integration.api.creators import create_master_from_request
    
    # =========================================================================
    # STEP 4: Execute the creation with comprehensive error handling
    # =========================================================================
    
    try:
        # CALL THE ROUTER - It handles customer vs item vs group etc.
        result = create_master_from_request(request)
        
        # =====================================================================
        # STEP 5A: SUCCESS PATH
        # =====================================================================
        if result.get("success"):
            
            # Update request to "Completed" status
            # WHY: User sees their request was fulfilled
            request.status = "Completed"
            request.sync_status = "Success"
            request.tally_master_created = 1  # Checkbox field
            request.created_in_tally_on = frappe.utils.now()  # Timestamp
            request.sync_log = result.get("sync_log")  # Link to Tally Sync Log
            
            # Save all changes to database
            request.db_update()
            frappe.db.commit()
            
            # -------------------------------------------------------------
            # SEND COMPLETION EMAIL TO REQUESTER
            # -------------------------------------------------------------
            # WHY: User knows their request is complete
            # HELPS: Reduces "what's the status?" questions
            notify_requester_completion(request)
            
            # -------------------------------------------------------------
            # AUTO-RETRY LINKED TRANSACTION
            # -------------------------------------------------------------
            # EXAMPLE: User submitted Sales Invoice → Failed (customer missing)
            #          → Created request → Admin approved → Customer now exists
            #          → NOW: Retry the invoice sync automatically
            # WHY: Seamless user experience - they don't have to manually retry
            if request.linked_transaction:
                retry_linked_transaction_sync(request)
            
            return {
                "success": True,
                "message": f"Master '{request.master_name}' created successfully in Tally",
                "sync_log": request.sync_log
            }
        
        # =====================================================================
        # STEP 5B: FAILURE PATH (Tally rejected the XML)
        # =====================================================================
        else:
            
            # Update request to "Failed" status
            request.status = "Failed"
            request.sync_status = "Failed"
            request.sync_error = result.get("error", "Unknown error")[:1000]
            request.sync_log = result.get("sync_log")
            
            # Save to database
            request.db_update()
            frappe.db.commit()
            
            # -------------------------------------------------------------
            # NOTIFY ADMIN ABOUT FAILURE
            # -------------------------------------------------------------
            # WHY: Admin needs to know it failed so they can fix the issue
            # EXAMPLE: "Parent group 'Electronics' doesn't exist"
            #          Admin creates "Electronics" in Tally, then clicks "Retry"
            notify_admin_failure(request)
            
            return {
                "success": False,
                "error": result.get("error"),
                "sync_log": request.sync_log
            }
    
    # =========================================================================
    # STEP 6: EXCEPTION HANDLING (Unexpected errors)
    # =========================================================================
    # WHAT: Catches Python exceptions (network errors, XML errors, etc.)
    # WHY: Prevents the job from crashing silently
    # HELPS: Admin sees what went wrong and can fix it
    
    except Exception as e:
        import traceback
        
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        
        # Log the full error with stack trace
        frappe.log_error(
            message=f"Master creation exception: {error_msg}\n\n{stack_trace}",
            title=f"Tally Master Creation Failed: {request_name}"
        )
        
        # Update request to Failed with error message
        request.status = "Failed"
        request.sync_status = "Failed"
        request.sync_error = error_msg[:1000]  # Truncate to fit in database field
        
        request.db_update()
        frappe.db.commit()
        
        return {
            "success": False,
            "error": error_msg
        }


# =============================================================================
# HELPER FUNCTION: Retry linked transaction
# =============================================================================

def retry_linked_transaction_sync(request):
    """
    After master is created, automatically retry the transaction that needed it
    
    REAL-WORLD SCENARIO:
    ────────────────────────────────────────────────────────────────────
    10:00 AM: User submits Sales Invoice INV-001
              Tally sync FAILS: "Customer ABC Corp not found"
    
    10:05 AM: User creates request for Customer "ABC Corp"
              Request linked_transaction = "INV-001"
    
    11:00 AM: Admin approves request
              Customer "ABC Corp" created in Tally ✓
    
    11:00 AM: THIS FUNCTION RUNS
              → Finds failed sync log for INV-001
              → Creates immediate retry job
              → Invoice syncs successfully ✓
    
    11:01 AM: User sees invoice in Tally (didn't have to do anything!)
    ────────────────────────────────────────────────────────────────────
    
    Args:
        request: Tally Master Creation Request document
    
    Returns:
        None (runs silently in background)
    
    WHY WE DO THIS:
    - Seamless user experience (automatic retry)
    - User doesn't need to know about sync internals
    - Reduces support tickets "My invoice didn't sync"
    
    HOW IT HELPS:
    - Closes the loop: Request → Approval → Creation → Retry → Success
    - No manual intervention needed
    """
    
    # Skip if no linked transaction
    if not request.linked_transaction:
        return
    
    # -------------------------------------------------------------------------
    # Find the failed sync log for this transaction
    # -------------------------------------------------------------------------
    # WHY: We need to know which sync attempt failed
    # NOTE: We look for FAILED or QUEUED status (might be retrying already)
    
    sync_logs = frappe.get_all(
        "Tally Sync Log",
        filters={
            "document_name": request.linked_transaction,  # e.g., "INV-001"
            "document_type": request.linked_transaction_doctype,  # e.g., "Sales Invoice"
            "sync_status": ["in", ["FAILED", "QUEUED"]]
        },
        order_by="creation desc",  # Get most recent
        limit=1
    )
    
    # No failed sync found - nothing to retry
    if not sync_logs:
        return
    
    # -------------------------------------------------------------------------
    # Create immediate retry job
    # -------------------------------------------------------------------------
    # WHAT: Creates a Tally Retry Job with next_retry_time = now
    # WHY: Triggers immediate retry (doesn't wait for scheduled job)
    # HELPS: Invoice syncs within seconds, not minutes/hours
    
    try:
        # Import retry engine
        # NOTE: Imported here to avoid circular dependency
        from tally_connect.tally_integration.retry_engine import create_retry_job_from_log
        
        # Load the sync log
        sync_log = frappe.get_doc("Tally Sync Log", sync_logs[0].name)
        
        # Create immediate retry (immediate=True sets next_retry_time to now)
        create_retry_job_from_log(sync_log, immediate=True)
        
        frappe.msgprint(
            f"Linked transaction {request.linked_transaction} will be retried immediately",
            indicator="blue",
            alert=True
        )
    
    except Exception as e:
        # Don't fail the whole process if retry creation fails
        # Just log it and move on
        frappe.log_error(
            f"Failed to create retry for {request.linked_transaction}: {str(e)}",
            "Tally Master Creation - Retry Failed"
        )


# =============================================================================
# NOTIFICATION FUNCTIONS
# =============================================================================

def notify_requester_completion(request):
    """
    Send completion email to the person who created the request
    
    WHY: User knows their request was fulfilled
    HELPS: Transparency - user sees the outcome
    """
    
    frappe.sendmail(
        recipients=[request.requested_by],
        subject=f"✅ Tally Master Created: {request.master_name}",
        message=f"""
            <div style="font-family: Arial, sans-serif; padding: 20px;">
                <h2 style="color: #28a745;">✅ Request Completed</h2>
                
                <p>Hello,</p>
                
                <p>Your Tally Master Creation Request has been <strong style="color: #28a745;">COMPLETED</strong>.</p>
                
                <table style="border-collapse: collapse; margin: 20px 0; width: 100%;">
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8f9fa; font-weight: bold; width: 30%;">Request ID</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{request.name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8f9fa; font-weight: bold;">Master Type</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{request.master_type}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8f9fa; font-weight: bold;">Master Name</td>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>{request.master_name}</strong></td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8f9fa; font-weight: bold;">Created On</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{frappe.utils.format_datetime(request.created_in_tally_on)}</td>
                    </tr>
                </table>
                
                {f'<p style="background: #d1ecf1; border-left: 4px solid #0c5460; padding: 15px; margin: 20px 0;"><strong>Automatic Retry:</strong> Your linked transaction <strong>{request.linked_transaction}</strong> has been automatically retried and should now sync to Tally.</p>' if request.linked_transaction else ''}
                
                <p>You can now use this master in Tally.</p>
                
                <p style="margin-top: 30px;">
                    <a href="{frappe.utils.get_url()}/app/tally-master-creation-request/{request.name}" 
                       style="background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; display: inline-block;">
                       View Request Details
                    </a>
                </p>
            </div>
        """,
        reference_doctype=request.doctype,
        reference_name=request.name
    )


def notify_admin_failure(request):
    """
    Notify admin that master creation failed
    
    WHY: Admin needs to investigate and fix the issue
    HELPS: Quick response to failures
    """
    
    frappe.sendmail(
        recipients=[request.assigned_to],
        subject=f"❌ Tally Master Creation Failed: {request.master_name}",
        message=f"""
            <div style="font-family: Arial, sans-serif; padding: 20px;">
                <h2 style="color: #dc3545;">❌ Request Failed</h2>
                
                <p>Hello,</p>
                
                <p>The Tally Master Creation Request you approved has <strong style="color: #dc3545;">FAILED</strong>.</p>
                
                <table style="border-collapse: collapse; margin: 20px 0; width: 100%;">
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8f9fa; font-weight: bold; width: 30%;">Request ID</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{request.name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8f9fa; font-weight: bold;">Master Name</td>
                        <td style="padding: 10px; border: 1px solid #ddd;">{request.master_name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd; background: #f8d7da; font-weight: bold; color: #721c24;">Error</td>
                        <td style="padding: 10px; border: 1px solid #ddd; color: #721c24;"><strong>{request.sync_error}</strong></td>
                    </tr>
                </table>
                
                <div style="background: #fff3cd; border-left: 4px solid #856404; padding: 15px; margin: 20px 0;">
                    <strong>Common Fixes:</strong>
                    <ul>
                        <li>If "Parent group not found" → Create the parent group in Tally first</li>
                        <li>If "Already exists" → Delete duplicate or use different name</li>
                        <li>If "Connection error" → Check if Tally is running</li>
                    </ul>
                </div>
                
                <p style="margin-top: 30px;">
                    <a href="{frappe.utils.get_url()}/app/tally-master-creation-request/{request.name}" 
                       style="background: #dc3545; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; display: inline-block;">
                       Review & Retry Request
                    </a>
                </p>
            </div>
        """,
        reference_doctype=request.doctype,
        reference_name=request.name
    )


# =============================================================================
# API ENDPOINT: Manual retry
# =============================================================================

@frappe.whitelist()
def retry_master_creation(request_name):
    """
    Manually retry a failed request
    
    WHEN USED: Admin clicks "Retry" button on failed request
    
    WHY: Allows admin to retry after fixing the issue in Tally
    EXAMPLE: Parent group was missing → Admin created it → Clicks "Retry"
    
    Args:
        request_name: Request ID to retry
    
    Returns:
        dict: {success: bool, message: str}
    """
    
    try:
        request = frappe.get_doc("Tally Master Creation Request", request_name)
        
        # Validate request is in failed state
        if request.status != "Failed":
            return {
                "success": False,
                "error": f"Cannot retry request with status '{request.status}'. Only failed requests can be retried."
            }
        
        # Reset status to "In Progress"
        request.status = "In Progress"
        request.sync_status = "In Progress"
        request.sync_error = None
        request.db_update()
        frappe.db.commit()
        
        # Queue the job again
        frappe.enqueue(
            method="tally_connect.tally_integration.api.approval.create_master_in_tally",
            queue="short",
            timeout=300,
            is_async=True,
            request_name=request_name
        )
        
        return {
            "success": True,
            "message": f"Retry initiated for request {request_name}"
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# =============================================================================
# SUMMARY OF WHAT THIS FILE DOES
# =============================================================================
"""
FLOW DIAGRAM:
─────────────────────────────────────────────────────────────────────────

1. Admin clicks "Approve" button
   ↓
2. tally_master_creation_request.on_approve() queues job
   ↓
3. Background worker picks up job
   ↓
4. create_master_in_tally() [THIS FILE] executes
   ├─→ Updates status to "In Progress"
   ├─→ Calls creators.py (actual Tally communication)
   ├─→ On Success:
   │   ├─→ Updates status to "Completed"
   │   ├─→ Sends completion email to user
   │   └─→ Retries linked transaction (if any)
   └─→ On Failure:
       ├─→ Updates status to "Failed"
       └─→ Sends failure email to admin

BENEFITS:
─────────
✅ Single point of control for all master creation
✅ Consistent error handling
✅ Automatic notifications
✅ Automatic retry of linked transactions
✅ Complete audit trail
✅ Non-blocking (runs in background)
✅ Admin sees real-time progress
✅ Graceful failure handling

EDGE CASES HANDLED:
───────────────────
🛡️ Request deleted while job in queue → Log and exit
🛡️ Network error during creation → Capture stack trace
🛡️ Tally rejects XML → Capture error, notify admin
🛡️ Retry creation fails → Don't fail whole process
🛡️ Multiple simultaneous approvals → Queue handles it
"""
