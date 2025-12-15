// Copyright (c) 2025, Kunal Verma and contributors
// For license information, please see license.txt

// tally_connect/tally_integration/doctype/tally_master_creation_request/tally_master_creation_request.js

frappe.ui.form.on('Tally Master Creation Request', {
    refresh: function(frm) {
        // Set field properties
        frm.set_df_property('erpnext_data', 'options', 'JSON');
        frm.set_df_property('notification_history', 'options', 'JSON');
        
        // Add custom buttons based on status
        if (frm.doc.status === "Pending Approval" && frappe.user.has_role("Tally Admin")) {
            frm.add_custom_button(__('Approve'), function() {
                show_approval_dialog(frm);
            }).addClass('btn-primary');
            
            frm.add_custom_button(__('Reject'), function() {
                show_rejection_dialog(frm);
            }).addClass('btn-danger');
        }
        
        if (frm.doc.status === "Failed") {
            frm.add_custom_button(__('Retry'), function() {
                retry_creation(frm);
            });
        }
        
        // Add button to view ERPNext source document
        if (frm.doc.erpnext_doctype && frm.doc.erpnext_document) {
            frm.add_custom_button(__('View Source Document'), function() {
                frappe.set_route("Form", frm.doc.erpnext_doctype, frm.doc.erpnext_document);
            }, __('Actions'));
        }
        
        // Add button to view linked transaction
        if (frm.doc.linked_transaction && frm.doc.linked_transaction_doctype) {
            frm.add_custom_button(__('View Linked Transaction'), function() {
                frappe.set_route("Form", frm.doc.linked_transaction_doctype, frm.doc.linked_transaction);
            }, __('Actions'));
        }
        
        // Add button to view sync log
        if (frm.doc.sync_log) {
            frm.add_custom_button(__('View Sync Log'), function() {
                frappe.set_route("Form", "Tally Sync Log", frm.doc.sync_log);
            }, __('Actions'));
        }
        
        // Add button to refresh ERPNext data
        frm.add_custom_button(__('Refresh ERPNext Data'), function() {
            refresh_erpnext_data(frm);
        }, __('Actions'));
        
        // Color-code status field
        set_status_indicator(frm);
        
        // Show notification timeline
        if (frm.doc.notification_history) {
            render_notification_timeline(frm);
        }
    },
    
    priority: function(frm) {
        // Show warning for urgent priority
        if (frm.doc.priority === "Urgent") {
            frappe.msgprint({
                title: __('Urgent Priority'),
                indicator: 'red',
                message: __('This request will be prioritized and assigned to senior admins.')
            });
        }
    },
    
    master_name: function(frm) {
        // Validate name length
        if (frm.doc.master_name && frm.doc.master_name.length > 100) {
            frappe.msgprint({
                title: __('Name Too Long'),
                indicator: 'orange',
                message: __('Master name exceeds 100 characters (Tally limit). Please shorten it.')
            });
        }
    }
});

function show_approval_dialog(frm) {
    let d = new frappe.ui.Dialog({
        title: __('Approve Request'),
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'info',
                options: `
                    <div style="padding: 10px; background: #d4edda; border-radius: 5px; margin-bottom: 15px;">
                        <p style="margin: 0; color: #155724;">
                            <strong>You are about to approve:</strong><br>
                            ${frm.doc.master_type}: <strong>${frm.doc.master_name}</strong>
                        </p>
                    </div>
                `
            },
            {
                fieldtype: 'Small Text',
                fieldname: 'approver_notes',
                label: __('Approver Notes (Optional)'),
                description: __('Add any comments or modifications you made')
            },
            {
                fieldtype: 'Section Break'
            },
            {
                fieldtype: 'Data',
                fieldname: 'modified_name',
                label: __('Modified Master Name (Optional)'),
                description: __('Change the master name if needed'),
                default: frm.doc.master_name
            },
            {
                fieldtype: 'Data',
                fieldname: 'modified_parent',
                label: __('Modified Parent Group (Optional)'),
                description: __('Change the parent group if needed'),
                default: frm.doc.parent_group
            }
        ],
        primary_action_label: __('Approve'),
        primary_action: function(values) {
            frappe.call({
                method: 'tally_connect.tally_integration.doctype.tally_master_creation_request.tally_master_creation_request.approve_request',
                args: {
                    request_name: frm.doc.name,
                    approver_notes: values.approver_notes,
                    modified_name: values.modified_name !== frm.doc.master_name ? values.modified_name : null,
                    modified_parent: values.modified_parent !== frm.doc.parent_group ? values.modified_parent : null
                },
                callback: function(r) {
                    if (r.message && r.message.success) {
                        frappe.show_alert({
                            message: __('Request approved successfully. Master creation in progress...'),
                            indicator: 'green'
                        }, 5);
                        frm.reload_doc();
                    }
                }
            });
            d.hide();
        }
    });
    
    d.show();
}

function show_rejection_dialog(frm) {
    let d = new frappe.ui.Dialog({
        title: __('Reject Request'),
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'warning',
                options: `
                    <div style="padding: 10px; background: #f8d7da; border-radius: 5px; margin-bottom: 15px;">
                        <p style="margin: 0; color: #721c24;">
                            <strong>⚠️ You are about to reject:</strong><br>
                            ${frm.doc.master_type}: <strong>${frm.doc.master_name}</strong>
                        </p>
                    </div>
                `
            },
            {
                fieldtype: 'Small Text',
                fieldname: 'rejection_reason',
                label: __('Rejection Reason'),
                reqd: 1,
                description: __('Explain why this request is being rejected')
            }
        ],
        primary_action_label: __('Reject'),
        primary_action: function(values) {
            frappe.call({
                method: 'tally_connect.tally_integration.doctype.tally_master_creation_request.tally_master_creation_request.reject_request',
                args: {
                    request_name: frm.doc.name,
                    rejection_reason: values.rejection_reason
                },
                callback: function(r) {
                    if (r.message && r.message.success) {
                        frappe.show_alert({
                            message: __('Request rejected'),
                            indicator: 'red'
                        }, 5);
                        frm.reload_doc();
                    }
                }
            });
            d.hide();
        }
    });
    
    d.show();
}

function retry_creation(frm) {
    frappe.confirm(
        __('Do you want to retry creating this master in Tally?'),
        function() {
            frappe.call({
                method: 'tally_connect.tally_integration.api.approval.retry_master_creation',
                args: {
                    request_name: frm.doc.name
                },
                callback: function(r) {
                    if (r.message && r.message.success) {
                        frappe.show_alert({
                            message: __('Retry initiated'),
                            indicator: 'blue'
                        }, 5);
                        frm.reload_doc();
                    }
                }
            });
        }
    );
}

function refresh_erpnext_data(frm) {
    frappe.call({
        method: 'tally_connect.tally_integration.doctype.tally_master_creation_request.tally_master_creation_request.get_request_details',
        args: {
            request_name: frm.doc.name
        },
        callback: function(r) {
            if (r.message) {
                let old_data = JSON.parse(frm.doc.erpnext_data || "{}");
                let new_data = JSON.parse(r.message.current_erpnext_data || "{}");
                
                if (JSON.stringify(old_data) !== JSON.stringify(new_data)) {
                    frappe.msgprint({
                        title: __('Data Changed'),
                        indicator: 'orange',
                        message: __('ERPNext data has changed since this request was created. Review changes before approving.')
                    });
                    
                    // Show comparison
                    show_data_comparison(old_data, new_data);
                } else {
                    frappe.show_alert({
                        message: __('Data is up to date'),
                        indicator: 'green'
                    }, 3);
                }
            }
        }
    });
}

function show_data_comparison(old_data, new_data) {
    // TODO: Implement side-by-side comparison dialog
    console.log("Old Data:", old_data);
    console.log("New Data:", new_data);
}

function set_status_indicator(frm) {
    let indicator_map = {
        "Pending Approval": "orange",
        "Approved": "blue",
        "In Progress": "blue",
        "Completed": "green",
        "Rejected": "red",
        "Failed": "red"
    };
    
    frm.page.set_indicator(frm.doc.status, indicator_map[frm.doc.status]);
}

function render_notification_timeline(frm) {
    let history = JSON.parse(frm.doc.notification_history || "[]");
    
    if (history.length === 0) return;
    
    let html = '<div class="frappe-control"><label>Notification Timeline</label><div style="border: 1px solid #d1d8dd; border-radius: 4px; padding: 15px; max-height: 300px; overflow-y: auto;">';
    
    history.forEach(function(entry) {
        let icon = entry.event === "created" ? "📧" : 
                   entry.event === "approved" ? "✅" : 
                   entry.event === "rejected" ? "❌" : 
                   entry.event === "completed" ? "🎉" : "📬";
        
        html += `
            <div style="margin-bottom: 10px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                <div style="display: flex; justify-content: space-between;">
                    <span><strong>${icon} ${entry.event.toUpperCase()}</strong></span>
                    <span style="color: #6c757d; font-size: 0.9em;">${frappe.datetime.str_to_user(entry.timestamp)}</span>
                </div>
                <div style="margin-top: 5px; color: #495057;">
                    Sent to: <strong>${entry.recipient_name}</strong> (${entry.recipient})
                </div>
            </div>
        `;
    });
    
    html += '</div></div>';
    
    frm.get_field('notification_history').$wrapper.html(html);
}
