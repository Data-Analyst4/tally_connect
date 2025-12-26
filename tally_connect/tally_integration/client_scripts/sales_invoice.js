frappe.ui.form.on('Sales Invoice', {
    refresh: function(frm) {
        if (frm.doc.docstatus === 1) {  // Only for submitted invoices
            frm.add_custom_button(__('Push to Tally'), function() {
                frappe.call({
                    method: 'tally_connect.tally_integration.api.creators.create_clean_sales_invoice_in_tally',
                    args: { invoice_name: frm.doc.name },
                    freeze: true,
                    freeze_message: 'Syncing with Tally...',
                    callback: function(r) {
                        if (r.message && r.message.success) {
                            frappe.show_alert({
                                message: 'Successfully pushed to Tally!',
                                indicator: 'green'
                            }, 5);
                            frm.reload_doc();
                        } else {
                            frappe.msgprint({
                                title: 'Tally Push Failed',
                                indicator: 'red',
                                message: r.message ? r.message.error : 'Unknown error'
                            });
                        }
                    }
                });
            }, __('Tally'));
        }
    }
});
