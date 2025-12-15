// =============================================================================
// FILE: tally_connect/tally_integration/client_scripts/sales_order.js
//
// PURPOSE: Add "Check Tally Dependencies" button to Sales Order/Invoice
// =============================================================================

frappe.ui.form.on('Sales Order', {
    refresh: function(frm) {
        // Add button only in draft or after submit
        if (frm.doc.docstatus !== 2) {  // Not cancelled
            add_tally_dependency_button(frm);
        }
    }
});

frappe.ui.form.on('Sales Invoice', {
    refresh: function(frm) {
        if (frm.doc.docstatus !== 2) {
            add_tally_dependency_button(frm);
        }
    }
});

function add_tally_dependency_button(frm) {
    // Check Tally Dependencies button
    frm.add_custom_button(__('Check Tally Dependencies'), function() {
        check_tally_dependencies(frm);
    }, __('Tally'));
    
    // Auto-check on submit if in draft
    if (frm.doc.docstatus === 0 && !frm._tally_checked) {
        // Auto-check when user clicks submit (optional)
        // Uncomment to enable:
        // check_tally_dependencies(frm);
        // frm._tally_checked = true;
    }
}

function check_tally_dependencies(frm) {
    frappe.call({
        method: 'tally_connect.tally_integration.api.dependency_checker.check_dependencies_and_show_missing',
        args: {
            doctype: frm.doctype,
            docname: frm.doc.name,
            company: frm.doc.company
        },
        freeze: true,
        freeze_message: __('Checking Tally dependencies...'),
        callback: function(r) {
            if (r.message.has_missing) {
                show_missing_masters_dialog(frm, r.message.missing_masters);
            } else {
                frappe.show_alert({
                    message: __('All dependencies exist in Tally ✓'),
                    indicator: 'green'
                }, 5);
            }
        },
        error: function(r) {
            frappe.msgprint({
                title: __('Error'),
                message: __('Failed to check Tally dependencies. Check Error Log.'),
                indicator: 'red'
            });
        }
    });
}

function show_missing_masters_dialog(frm, missing_masters) {
    // Build HTML table of missing masters
    let html = `
        <div style="margin-bottom: 15px;">
            <p><strong>${missing_masters.length} master(s) missing in Tally:</strong></p>
        </div>
        <table class="table table-bordered" style="margin-bottom: 15px;">
            <thead>
                <tr>
                    <th>Type</th>
                    <th>Name</th>
                    <th>Parent Group</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    missing_masters.forEach(function(master) {
        html += `
            <tr>
                <td><span class="indicator ${master.priority === 'High' ? 'red' : 'orange'}">${master.type}</span></td>
                <td><strong>${master.display_name}</strong></td>
                <td>${master.parent}</td>
            </tr>
        `;
    });
    
    html += `
            </tbody>
        </table>
        <p style="color: #888; font-size: 12px;">
            <i class="fa fa-info-circle"></i> 
            Creating requests will send them for approval to Tally admin.
        </p>
    `;
    
    // Show dialog
    let d = new frappe.ui.Dialog({
        title: __('Tally Masters Missing'),
        size: 'large',
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'missing_masters_html',
                options: html
            }
        ],
        primary_action_label: __('Create Approval Requests'),
        primary_action: function() {
            create_master_requests(frm, missing_masters, d);
        },
        secondary_action_label: __('Cancel')
    });
    
    d.show();
}

function create_master_requests(frm, missing_masters, dialog) {
    frappe.call({
        method: 'tally_connect.tally_integration.api.dependency_checker.create_requests_for_missing_masters',
        args: {
            doctype: frm.doctype,
            docname: frm.doc.name,
            company: frm.doc.company,
            missing_masters_json: JSON.stringify(missing_masters)
        },
        freeze: true,
        freeze_message: __('Creating master creation requests...'),
        callback: function(r) {
            if (r.message.success) {
                dialog.hide();
                
                frappe.show_alert({
                    message: __(`${r.message.requests_created.length} request(s) created successfully`),
                    indicator: 'green'
                }, 7);
                
                // Show created requests
                frappe.msgprint({
                    title: __('Requests Created'),
                    message: __(`
                        <p>Created ${r.message.requests_created.length} Tally Master Creation Request(s).</p>
                        <p>These will be reviewed and approved by the Tally administrator.</p>
                        <p><a href="/app/tally-master-creation-request">View Requests →</a></p>
                    `),
                    indicator: 'blue'
                });
            }
        }
    });
}
