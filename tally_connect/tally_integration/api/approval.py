# FILE: tally_connect/tally_integration/api/approval.py

import frappe
from frappe import _

def create_master_in_tally(request_name):
    """
    Background job that creates master in Tally
    Called by RQ worker
    
    Args:
        request_name (str): Tally Master Creation Request ID
    
    Returns:
        dict: {success: bool, message: str}
    """
    
    # ========== STEP 1: LOAD REQUEST ==========
    try:
        request = frappe.get_doc("Tally Master Creation Request", request_name)
    except frappe.DoesNotExistError:
        frappe.log_error(f"Request {request_name} not found", "Tally Push Cycle")
        return {"success": False, "error": "Request not found"}
    
    # ========== STEP 2: UPDATE STATUS TO IN PROGRESS ==========
    request.status = "In Progress"
    request.sync_status = "In Progress"
    request.db_set("status", "In Progress", update_modified=True)
    request.db_set("sync_status", "In Progress", update_modified=True)
    frappe.db.commit()  # Commit immediately so status is visible
    
    # ========== STEP 3: ROUTE TO APPROPRIATE CREATOR ==========
    try:
        result = None
        
        if request.master_type == "Customer":
            from tally_connect.tally_integration.api.creators import create_customer_ledger_in_tally
            result = create_customer_ledger_in_tally(
                customer_name=request.erpnext_document,
                company=request.company
            )
        
        elif request.master_type == "Supplier":
            from tally_connect.tally_integration.api.creators import create_supplier_ledger_in_tally
            result = create_supplier_ledger_in_tally(
                supplier_name=request.erpnext_document,
                company=request.company
            )
        
        elif request.master_type == "Item":
            from tally_connect.tally_integration.api.creators import create_stock_item_in_tally
            result = create_stock_item_in_tally(
                item_code=request.erpnext_document,
                company=request.company
            )
        
        elif request.master_type == "Group":
            from tally_connect.tally_integration.api.creators import create_group_in_tally
            result = create_group_in_tally(
                group_name=request.master_name,
                parent_group=request.parent_group,
                company=request.company
            )
        
        elif request.master_type == "Unit":
            from tally_connect.tally_integration.api.creators import create_unit_in_tally
            result = create_unit_in_tally(
                unit_name=request.master_name,
                unit_type="Simple",
                company=request.company
            )
        
        else:
            raise ValueError(f"Unsupported master type: {request.master_type}")
        
        # ========== STEP 4: HANDLE SUCCESS ==========
        if result.get("success"):
            request.status = "Completed"
            request.sync_status = "Success"
            request.tally_master_created = 1
            request.created_in_tally_on = frappe.utils.now()
            request.sync_log = result.get("sync_log")
            
            request.db_update()
            frappe.db.commit()
            
            # Notify requester
            notify_requester_completion(request)
            
            # Retry linked transactions
            if request.linked_transaction:
                retry_linked_transaction_sync(request)
            
            return {
                "success": True,
                "message": f"Master '{request.master_name}' created successfully in Tally"
            }
        
        # ========== STEP 5: HANDLE FAILURE ==========
        else:
            request.status = "Failed"
            request.sync_status = "Failed"
            request.sync_error = result.get("error", "Unknown error")
            request.sync_log = result.get("sync_log")
            
            request.db_update()
            frappe.db.commit()
            
            # Notify admin about failure
            notify_admin_failure(request)
            
            return {
                "success": False,
                "error": result.get("error")
            }
    
    except Exception as e:
        # ========== STEP 6: HANDLE EXCEPTIONS ==========
        import traceback
        
        error_message = str(e)
        stack_trace = traceback.format_exc()
        
        frappe.log_error(
            message=f"Failed to create master: {error_message}\n\n{stack_trace}",
            title=f"Tally Master Creation Failed: {request_name}"
        )
        
        request.status = "Failed"
        request.sync_status = "Failed"
        request.sync_error = error_message
        
        request.db_update()
        frappe.db.commit()
        
        return {
            "success": False,
            "error": error_message
        }


def retry_linked_transaction_sync(request):
    """
    After master is created, retry the transaction that needed it
    """
    if not request.linked_transaction:
        return
    
    # Find the failed sync log
    sync_logs = frappe.get_all(
        "Tally Sync Log",
        filters={
            "document_name": request.linked_transaction,
            "document_type": request.linked_transaction_doctype,
            "sync_status": ["in", ["FAILED", "QUEUED"]]
        },
        order_by="creation desc",
        limit=1
    )
    
    if not sync_logs:
        return
    
    # Create immediate retry job
    from tally_connect.tally_integration.retry_engine import create_retry_job_from_log
    
    sync_log = frappe.get_doc("Tally Sync Log", sync_logs[0].name)
    create_retry_job_from_log(sync_log, immediate=True)


def notify_requester_completion(request):
    """
    Send completion email to requester
    """
    frappe.sendmail(
        recipients=[request.requested_by],
        subject=f"✅ Tally Master Created: {request.master_name}",
        message=f"""
            <p>Hello,</p>
            <p>Your Tally Master Creation Request has been <strong style="color: green;">COMPLETED</strong>.</p>
            <table style="border-collapse: collapse; margin: 20px 0;">
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Request ID</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{request.name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Master Type</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{request.master_type}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Master Name</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{request.master_name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Created On</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{frappe.utils.format_datetime(request.created_in_tally_on)}</td>
                </tr>
            </table>
            {f'<p>Your linked transaction <strong>{request.linked_transaction}</strong> has been automatically retried and should now sync to Tally.</p>' if request.linked_transaction else ''}
            <p>You can now use this master in Tally.</p>
        """,
        reference_doctype=request.doctype,
        reference_name=request.name
    )


def notify_admin_failure(request):
    """
    Notify admin about creation failure
    """
    frappe.sendmail(
        recipients=[request.assigned_to],
        subject=f"❌ Tally Master Creation Failed: {request.master_name}",
        message=f"""
            <p>Hello,</p>
            <p>The Tally Master Creation Request you approved has <strong style="color: red;">FAILED</strong>.</p>
            <table style="border-collapse: collapse; margin: 20px 0;">
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Request ID</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{request.name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Master Name</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{request.master_name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; color: red;"><strong>Error</strong></td>
                    <td style="padding: 8px; border: 1px solid #ddd; color: red;">{request.sync_error}</td>
                </tr>
            </table>
            <p>Please review the error and take appropriate action.</p>
            <p><a href="{frappe.utils.get_url()}/app/tally-master-creation-request/{request.name}" 
               style="background: #dc3545; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block;">
               Review Request
            </a></p>
        """,
        reference_doctype=request.doctype,
        reference_name=request.name
    )
