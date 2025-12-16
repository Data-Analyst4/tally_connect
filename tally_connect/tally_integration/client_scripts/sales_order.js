// // =============================================================================
// // FILE: tally_connect/tally_integration/client_scripts/sales_order.js
// //
// // PURPOSE: Add "Check Tally Dependencies" button to Sales Order/Invoice
// // =============================================================================

// frappe.ui.form.on('Sales Order', {
//     refresh: function(frm) {
//         // Add button only in draft or after submit
//         if (frm.doc.docstatus !== 2) {  // Not cancelled
//             add_tally_dependency_button(frm);
//         }
//     }
// });

// frappe.ui.form.on('Sales Invoice', {
//     refresh: function(frm) {
//         if (frm.doc.docstatus !== 2) {
//             add_tally_dependency_button(frm);
//         }
//     }
// });

// function add_tally_dependency_button(frm) {
//     // Check Tally Dependencies button
//     frm.add_custom_button(__('Check Tally Dependencies'), function() {
//         check_tally_dependencies(frm);
//     }, __('Tally'));
    
//     // Auto-check on submit if in draft
//     if (frm.doc.docstatus === 0 && !frm._tally_checked) {
//         // Auto-check when user clicks submit (optional)
//         // Uncomment to enable:
//         // check_tally_dependencies(frm);
//         // frm._tally_checked = true;
//     }
// }

// // function check_tally_dependencies(frm) {
// //     frappe.call({
// //         method: 'tally_connect.tally_integration.api.dependency_checker.check_dependencies_and_show_missing',
// //         args: {
// //             doctype: frm.doctype,
// //             docname: frm.doc.name,
// //             company: frm.doc.company
// //         },
// //         freeze: true,
// //         freeze_message: __('Checking Tally dependencies...'),
// //         callback: function(r) {
// //             if (r.message.has_missing) {
// //                 show_missing_masters_dialog(frm, r.message.missing_masters);
// //             } else {
// //                 frappe.show_alert({
// //                     message: __('All dependencies exist in Tally ✓'),
// //                     indicator: 'green'
// //                 }, 5);
// //             }
// //         },
// //         error: function(r) {
// //             frappe.msgprint({
// //                 title: __('Error'),
// //                 message: __('Failed to check Tally dependencies. Check Error Log.'),
// //                 indicator: 'red'
// //             });
// //         }
// //     });
// // }

// function check_tally_dependencies(frm) {
//     frappe.call({
//         method: 'tally_connect.tally_integration.api.dependency_checker.check_dependencies_and_show_missing',
//         args: {
//             doctype: frm.doctype,
//             docname: frm.doc.name,
//             company: frm.doc.company
//         },
//         freeze: true,
//         freeze_message: __('Checking Tally connection...'),
//         callback: function(r) {
//             // Check if there was an error (Tally not connected)
//             if (r.message.error) {
//                 frappe.msgprint({
//                     title: __('Tally Connection Error'),
//                     message: `
//                         <div style="margin-bottom: 10px;">
//                             <strong>${r.message.error}</strong>
//                         </div>
//                         <div style="color: #666; font-size: 13px;">
//                             ${r.message.error_details}
//                         </div>
//                         <hr style="margin: 15px 0;">
//                         <div style="font-size: 12px;">
//                             <strong>Please check:</strong>
//                             <ul>
//                                 <li>Tally is running</li>
//                                 <li>Port 9000 is open</li>
//                                 <li>Correct company is loaded</li>
//                             </ul>
//                         </div>
//                     `,
//                     indicator: 'red'
//                 });
//                 return;
//             }
            
//             // Tally connected - show results
//             if (r.message.has_missing) {
//                 show_missing_dialog(frm, r.message.missing_masters);
//             } else {
//                 frappe.show_alert({
//                     message: __('✅ All dependencies exist in Tally'),
//                     indicator: 'green'
//                 }, 5);
//             }
//         },
//         error: function(r) {
//             frappe.msgprint({
//                 title: __('Error'),
//                 message: __('Failed to check dependencies. Check Error Log.'),
//                 indicator: 'red'
//             });
//         }
//     });
// }

// // Keep rest of the functions same...

// function show_missing_masters_dialog(frm, missing_masters) {
//     // Build HTML table of missing masters
//     let html = `
//         <div style="margin-bottom: 15px;">
//             <p><strong>${missing_masters.length} master(s) missing in Tally:</strong></p>
//         </div>
//         <table class="table table-bordered" style="margin-bottom: 15px;">
//             <thead>
//                 <tr>
//                     <th>Type</th>
//                     <th>Name</th>
//                     <th>Parent Group</th>
//                 </tr>
//             </thead>
//             <tbody>
//     `;
    
//     missing_masters.forEach(function(master) {
//         html += `
//             <tr>
//                 <td><span class="indicator ${master.priority === 'High' ? 'red' : 'orange'}">${master.type}</span></td>
//                 <td><strong>${master.display_name}</strong></td>
//                 <td>${master.parent}</td>
//             </tr>
//         `;
//     });
    
//     html += `
//             </tbody>
//         </table>
//         <p style="color: #888; font-size: 12px;">
//             <i class="fa fa-info-circle"></i> 
//             Creating requests will send them for approval to Tally admin.
//         </p>
//     `;
    
//     // Show dialog
//     let d = new frappe.ui.Dialog({
//         title: __('Tally Masters Missing'),
//         size: 'large',
//         fields: [
//             {
//                 fieldtype: 'HTML',
//                 fieldname: 'missing_masters_html',
//                 options: html
//             }
//         ],
//         primary_action_label: __('Create Approval Requests'),
//         primary_action: function() {
//             create_master_requests(frm, missing_masters, d);
//         },
//         secondary_action_label: __('Cancel')
//     });
    
//     d.show();
// }

// function create_master_requests(frm, missing_masters, dialog) {
//     frappe.call({
//         method: 'tally_connect.tally_integration.api.dependency_checker.create_requests_for_missing_masters',
//         args: {
//             doctype: frm.doctype,
//             docname: frm.doc.name,
//             company: frm.doc.company,
//             missing_masters_json: JSON.stringify(missing_masters)
//         },
//         freeze: true,
//         freeze_message: __('Creating master creation requests...'),
//         callback: function(r) {
//             if (r.message.success) {
//                 dialog.hide();
                
//                 frappe.show_alert({
//                     message: __(`${r.message.requests_created.length} request(s) created successfully`),
//                     indicator: 'green'
//                 }, 7);
                
//                 // Show created requests
//                 frappe.msgprint({
//                     title: __('Requests Created'),
//                     message: __(`
//                         <p>Created ${r.message.requests_created.length} Tally Master Creation Request(s).</p>
//                         <p>These will be reviewed and approved by the Tally administrator.</p>
//                         <p><a href="/app/tally-master-creation-request">View Requests →</a></p>
//                     `),
//                     indicator: 'blue'
//                 });
//             }
//         }
//     });
// }

// =============================================================================
// Tally Dependency Check - Sales Order & Sales Invoice
// =============================================================================

frappe.ui.form.on('Sales Order', {
    refresh: function(frm) {
        if (frm.doc.docstatus !== 2) {
            add_tally_check_button(frm);
        }
    }
});

frappe.ui.form.on('Sales Invoice', {
    refresh: function(frm) {
        if (frm.doc.docstatus !== 2) {
            add_tally_check_button(frm);
        }
    }
});

function add_tally_check_button(frm) {
    frm.add_custom_button(__('Check Tally Dependencies'), function() {
        check_tally_dependencies(frm);
    }, __('Tally'));
}

function check_tally_dependencies(frm) {
    console.log("🔍 Checking Tally dependencies...");
    
    frappe.call({
        method: 'tally_connect.tally_integration.api.dependency_checker.check_dependencies_and_show_missing',
        args: {
            doctype: frm.doctype,
            docname: frm.doc.name,
            company: frm.doc.company
        },
        freeze: true,
        freeze_message: __('Checking Tally...'),
        callback: function(r) {
            console.log("✅ Check result:", r.message);
            
            if (r.message.error) {
                // Tally connection error
                frappe.msgprint({
                    title: __('Tally Connection Error'),
                    message: `
                        <div style="margin-bottom: 10px;">
                            <strong>${r.message.error}</strong>
                        </div>
                        <div style="color: #666; font-size: 13px;">
                            ${r.message.error_details || ''}
                        </div>
                        <hr style="margin: 15px 0;">
                        <div style="font-size: 12px;">
                            <strong>Please check:</strong>
                            <ul>
                                <li>Tally is running</li>
                                <li>Port 9000 is open</li>
                                <li>Correct company is loaded</li>
                            </ul>
                        </div>
                    `,
                    indicator: 'red'
                });
                return;
            }
            
            if (r.message.has_missing) {
                show_missing_masters_dialog(frm, r.message.missing_masters);
            } else {
                frappe.show_alert({
                    message: __('✅ All dependencies exist in Tally'),
                    indicator: 'green'
                }, 5);
            }
        },
        error: function(r) {
            console.error("❌ Check failed:", r);
            frappe.msgprint({
                title: __('Error'),
                message: __('Failed to check dependencies. Check Error Log.'),
                indicator: 'red'
            });
        }
    });
}

function show_missing_masters_dialog(frm, missing_masters) {
    console.log("📋 Missing masters:", missing_masters);
    
    let html = `
        <div style="margin-bottom: 15px;">
            <p><strong>${missing_masters.length} master(s) missing in Tally:</strong></p>
        </div>
        <table class="table table-bordered">
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
        let badge_color = master.priority === 'High' ? 'red' : 'orange';
        html += `
            <tr>
                <td><span class="indicator ${badge_color}">${master.type}</span></td>
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
            Requests will be sent to Tally admin for approval.
        </p>
    `;
    
    let dialog = new frappe.ui.Dialog({
        title: __('Tally Masters Missing'),
        size: 'large',
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'missing_html',
                options: html
            }
        ],
        primary_action_label: __('Create Approval Requests'),
        primary_action: function() {
            create_approval_requests(frm, missing_masters, dialog);
        },
        secondary_action_label: __('Cancel')
    });
    
    dialog.show();
}

function create_approval_requests(frm, missing_masters, dialog) {
    console.log("📝 Creating requests for:", missing_masters);
    
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
            console.log("✅ Create result:", r.message);
            
            if (r.message && r.message.success) {
                dialog.hide();
                
                frappe.show_alert({
                    message: __(`${r.message.requests_created.length} request(s) created successfully`),
                    indicator: 'green'
                }, 7);
                
                frappe.msgprint({
                    title: __('Requests Created'),
                    message: `
                        <p>Created <strong>${r.message.requests_created.length}</strong> Tally Master Creation Request(s).</p>
                        <p>These will be reviewed and approved by the Tally administrator.</p>
                        <hr>
                        <p><a href="/app/tally-master-creation-request" target="_blank">
                            <i class="fa fa-external-link"></i> View Requests
                        </a></p>
                    `,
                    indicator: 'blue'
                });
            } else {
                dialog.hide();
                
                let error_msg = r.message ? r.message.message : 'Unknown error';
                
                frappe.msgprint({
                    title: __('Error Creating Requests'),
                    message: `
                        <p><strong>Failed to create some requests:</strong></p>
                        <p>${error_msg}</p>
                        <hr>
                        <p><a href="/app/error-log" target="_blank">
                            <i class="fa fa-bug"></i> View Error Log
                        </a></p>
                    `,
                    indicator: 'red'
                });
            }
        },
        error: function(r) {
            console.error("❌ Create failed:", r);
            
            dialog.hide();
            
            frappe.msgprint({
                title: __('Error'),
                message: __('Failed to create requests. Check browser console and Error Log.'),
                indicator: 'red'
            });
        }
    });
}

console.log("✅ Tally dependency checker loaded");
