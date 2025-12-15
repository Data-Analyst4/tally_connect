# Copyright (c) 2025, Kunal Verma and contributors
# For license information, please see license.txt

# tally_connect/tally_integration/doctype/tally_master_creation_request/tally_master_creation_request.py

import frappe
from frappe import _
from frappe.model.document import Document
import json
from datetime import datetime

class TallyMasterCreationRequest(Document):
    """
    Controller for Tally Master Creation Request DocType
    
    Handles:
    - Validation before save
    - Status transitions
    - Auto-assignment to admins
    - Notification triggers
    """
    
    def before_insert(self):
        """
        Called before document is inserted into database
        """
        # Set default values
        self.request_date = frappe.utils.now()
        self.requested_by = frappe.session.user
        self.status = "Pending Approval"
        
        # Capture ERPNext data snapshot
        if self.erpnext_doctype and self.erpnext_document:
            self.erpnext_data = self.capture_erpnext_data()
        
        # Determine master name if not provided
        if not self.master_name:
            self.master_name = self.get_suggested_master_name()
        
        # Determine parent group if not provided
        if not self.parent_group:
            self.parent_group = self.get_default_parent_group()
        
        # Auto-assign to Tally Admin
        if not self.assigned_to:
            self.assigned_to = self.get_next_available_admin()
        
        # Initialize notification history
        self.notification_history = json.dumps([])
    
    def after_insert(self):
        """
        Called after document is saved to database
        """
        # Send notification to assigned admin
        self.notify_assigned_admin()
        
        # Log notification
        self.add_notification_entry("created", self.assigned_to)
        
        # Publish real-time update
        frappe.publish_realtime(
            event="new_tally_request",
            message={
                "request_id": self.name,
                "master_name": self.master_name,
                "priority": self.priority
            },
            user=self.assigned_to
        )
    
    def validate(self):
        """
        Validation before save
        """
        # Validate status transitions
        if self.is_new():
            return  # Skip validation for new documents
        
        old_doc = self.get_doc_before_save()
        if old_doc and old_doc.status != self.status:
            self.validate_status_transition(old_doc.status, self.status)
        
        # Validate ERPNext document exists
        if self.erpnext_doctype and self.erpnext_document:
            if not frappe.db.exists(self.erpnext_doctype, self.erpnext_document):
                frappe.throw(
                    _("ERPNext {0} '{1}' does not exist").format(
                        self.erpnext_doctype, 
                        self.erpnext_document
                    )
                )
        
        # Validate master_name length (Tally limit)
        if len(self.master_name or "") > 100:
            frappe.throw(_("Master name exceeds 100 characters (Tally limit)"))
    
    def on_update(self):
        """
        Called after document is updated
        """
        # Check if status changed
        if self.has_value_changed("status"):
            self.handle_status_change()
    
    def validate_status_transition(self, old_status, new_status):
        """
        Ensure only valid status transitions
        """
        valid_transitions = {
            "Pending Approval": ["Approved", "Rejected"],
            "Approved": ["In Progress"],
            "In Progress": ["Completed", "Failed"],
            "Rejected": [],  # Terminal state
            "Completed": [],  # Terminal state
            "Failed": ["In Progress"]  # Can retry
        }
        
        allowed = valid_transitions.get(old_status, [])
        if new_status not in allowed:
            frappe.throw(
                _("Invalid status transition from '{0}' to '{1}'. Allowed: {2}").format(
                    old_status, 
                    new_status, 
                    ", ".join(allowed)
                )
            )
    
    def handle_status_change(self):
        """
        Trigger actions based on status change
        """
        if self.status == "Approved":
            self.on_approve()
        elif self.status == "Rejected":
            self.on_reject()
        elif self.status == "Completed":
            self.on_complete()
        elif self.status == "Failed":
            self.on_failure()
    
    def on_approve(self):
        """
        Called when request is approved
        """
        self.approval_date = frappe.utils.now()
        self.approved_by = frappe.session.user
        
        # Notify requester
        self.notify_requester_approval()
        
        # Log notification
        self.add_notification_entry("approved", self.requested_by)
        
        # Queue master creation job
        frappe.enqueue(
            method="tally_connect.tally_integration.api.approval.create_master_in_tally",
            queue="short",
            timeout=300,
            is_async=True,
            request_name=self.name
        )
    
    def on_reject(self):
        """
        Called when request is rejected
        """
        self.rejection_date = frappe.utils.now()
        self.rejected_by = frappe.session.user
        
        # Notify requester
        self.notify_requester_rejection()
        
        # Log notification
        self.add_notification_entry("rejected", self.requested_by)
    
    def on_complete(self):
        """
        Called when master creation is completed
        """
        self.created_in_tally_on = frappe.utils.now()
        self.tally_master_created = 1
        
        # Notify requester
        self.notify_requester_completion()
        
        # Retry linked transaction if exists
        if self.linked_transaction:
            self.retry_linked_transaction()
    
    def on_failure(self):
        """
        Called when master creation fails
        """
        # Notify assigned admin about failure
        self.notify_admin_failure()
    
    # ========== HELPER METHODS ==========
    
    def capture_erpnext_data(self):
        """
        Take snapshot of ERPNext document data
        """
        try:
            doc = frappe.get_doc(self.erpnext_doctype, self.erpnext_document)
            
            data = {
                "doctype": doc.doctype,
                "name": doc.name,
                "modified": str(doc.modified),
                "snapshot_taken_at": frappe.utils.now()
            }
            
            if doc.doctype == "Customer":
                data["details"] = {
                    "customer_name": doc.customer_name,
                    "customer_type": doc.customer_type,
                    "customer_group": doc.customer_group,
                    "territory": doc.territory,
                    "gst_category": getattr(doc, "gst_category", None),
                    "gstin": getattr(doc, "gstin", None)
                }
            
            elif doc.doctype == "Item":
                data["details"] = {
                    "item_code": doc.item_code,
                    "item_name": doc.item_name,
                    "item_group": doc.item_group,
                    "stock_uom": doc.stock_uom,
                    "is_stock_item": doc.is_stock_item,
                    "gst_hsn_code": getattr(doc, "gst_hsn_code", None)
                }
            
            return json.dumps(data, indent=2, default=str)
        
        except Exception as e:
            frappe.log_error(f"Failed to capture ERPNext data: {str(e)}")
            return json.dumps({"error": str(e)})
    
    def get_suggested_master_name(self):
        """
        Suggest Tally-compliant name from ERPNext document
        """
        if not self.erpnext_doctype or not self.erpnext_document:
            return None
        
        doc = frappe.get_doc(self.erpnext_doctype, self.erpnext_document)
        
        if self.erpnext_doctype == "Customer":
            name = doc.customer_name
        elif self.erpnext_doctype == "Item":
            name = doc.item_code  # Use code for items (more stable)
        else:
            name = doc.name
        
        # Sanitize for Tally
        name = self.sanitize_for_tally(name)
        
        # Truncate if too long
        if len(name) > 100:
            name = name[:97] + "..."
        
        return name
    
    def sanitize_for_tally(self, name):
        """
        Remove/replace characters not allowed in Tally
        """
        replacements = {
            "&": "and",
            "<": "",
            ">": "",
            '"': "",
            "'": ""
        }
        
        for char, replacement in replacements.items():
            name = name.replace(char, replacement)
        
        return name
    
    def get_default_parent_group(self):
        """
        Determine default parent group based on master type
        """
        from tally_connect.tally_integration.utils import get_settings
        settings = get_settings()
        
        parent_map = {
            "Customer": settings.default_customer_ledger or "Sundry Debtors",
            "Supplier": settings.default_supplier_ledger or "Sundry Creditors",
            "Item": settings.default_inventory_stock_group or "Primary",
            "Stock Group": "Primary",
            "Unit": None,
            "Godown": None
        }
        
        return parent_map.get(self.master_type)
    
    def get_next_available_admin(self):
        """
        Get next available Tally Admin using round-robin
        """
        from tally_connect.tally_integration.workflows.assignment import get_next_available_tally_admin
        return get_next_available_tally_admin(self.priority)
    
    def add_notification_entry(self, event_type, recipient):
        """
        Add entry to notification history
        """
        history = json.loads(self.notification_history or "[]")
        
        history.append({
            "timestamp": frappe.utils.now(),
            "event": event_type,
            "recipient": recipient,
            "recipient_name": frappe.db.get_value("User", recipient, "full_name"),
            "notification_type": "email"
        })
        
        self.notification_history = json.dumps(history, indent=2)
        self.save(ignore_permissions=True)
    
    def retry_linked_transaction(self):
        """
        Trigger retry of linked transaction sync
        """
        if not self.linked_transaction:
            return
        
        from tally_connect.tally_integration.api.approval import retry_linked_transaction_sync
        retry_linked_transaction_sync(self)
    
    # ========== NOTIFICATION METHODS ==========
    
    def notify_assigned_admin(self):
        """
        Send notification to assigned admin
        """
        from tally_connect.tally_integration.workflows.notification import notify_admin_new_request
        notify_admin_new_request(self)
    
    def notify_requester_approval(self):
        """
        Send approval notification to requester
        """
        from tally_connect.tally_integration.workflows.notification import notify_requester_approval
        notify_requester_approval(self)
    
    def notify_requester_rejection(self):
        """
        Send rejection notification to requester
        """
        from tally_connect.tally_integration.workflows.notification import notify_requester_rejection
        notify_requester_rejection(self)
    
    def notify_requester_completion(self):
        """
        Send completion notification to requester
        """
        from tally_connect.tally_integration.workflows.notification import notify_requester_completion
        notify_requester_completion(self)
    
    def notify_admin_failure(self):
        """
        Send failure notification to admin
        """
        from tally_connect.tally_integration.workflows.notification import notify_admin_failure
        notify_admin_failure(self)


# ========== WHITELISTED API METHODS ==========

@frappe.whitelist()
def approve_request(request_name, approver_notes=None, modified_name=None, modified_parent=None):
    """
    Approve a request (called from UI)
    """
    request = frappe.get_doc("Tally Master Creation Request", request_name)
    
    # Check permissions
    if not frappe.has_permission(request.doctype, "write", doc=request):
        frappe.throw(_("You don't have permission to approve this request"))
    
    # Apply modifications if provided
    if modified_name:
        request.modified_master_name = modified_name
        request.master_name = modified_name
    
    if modified_parent:
        request.modified_parent_group = modified_parent
        request.parent_group = modified_parent
    
    if approver_notes:
        request.approver_notes = approver_notes
    
    # Change status (will trigger on_approve)
    request.status = "Approved"
    request.save(ignore_permissions=True)
    frappe.db.commit()
    
    return {"success": True, "message": "Request approved successfully"}


@frappe.whitelist()
def reject_request(request_name, rejection_reason):
    """
    Reject a request (called from UI)
    """
    if not rejection_reason:
        frappe.throw(_("Rejection reason is mandatory"))
    
    request = frappe.get_doc("Tally Master Creation Request", request_name)
    
    # Check permissions
    if not frappe.has_permission(request.doctype, "write", doc=request):
        frappe.throw(_("You don't have permission to reject this request"))
    
    request.rejection_reason = rejection_reason
    request.status = "Rejected"
    request.save(ignore_permissions=True)
    frappe.db.commit()
    
    return {"success": True, "message": "Request rejected"}


@frappe.whitelist()
def get_request_details(request_name):
    """
    Get detailed request information with live data
    """
    request = frappe.get_doc("Tally Master Creation Request", request_name)
    
    # Get current ERPNext data
    current_data = None
    if request.erpnext_doctype and request.erpnext_document:
        try:
            current_doc = frappe.get_doc(request.erpnext_doctype, request.erpnext_document)
            current_data = request.capture_erpnext_data()
        except:
            pass
    
    return {
        "request": request.as_dict(),
        "current_erpnext_data": current_data,
        "notification_history": json.loads(request.notification_history or "[]")
    }


@frappe.whitelist()
def get_my_pending_requests():
    """
    Get requests assigned to current user
    """
    user = frappe.session.user
    
    requests = frappe.get_all(
        "Tally Master Creation Request",
        filters={
            "assigned_to": user,
            "status": "Pending Approval"
        },
        fields=["name", "master_name", "master_type", "priority", "request_date", "requested_by"],
        order_by="FIELD(priority, 'Urgent', 'High', 'Normal', 'Low'), request_date ASC"
    )
    
    # Add requester names
    for req in requests:
        req["requester_name"] = frappe.db.get_value("User", req["requested_by"], "full_name")
    
    return requests
