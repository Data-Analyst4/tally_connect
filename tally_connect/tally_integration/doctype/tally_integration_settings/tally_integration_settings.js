// Copyright (c) 2025, Kunal Verma and contributors
// For license information, please see license.txt

// frappe.ui.form.on("Tally Integration Settings", {
// 	refresh(frm) {

// 	},
// });

frappe.ui.form.on('Tally Integration Settings', {
    test_connection_button: function(frm) {
        frappe.call({
            method: 'tally_connect.tally_integration.api.test_connection.test_connection',
            callback: function(r) {
                if (r.message && r.message.success) {
                    frappe.show_alert({
                        message: __('Connection Successful!'),
                        indicator: 'green'
                    });
                }
            }
        });
    }
});
