# ================================================================================
# FILE 5: sales_order.py - ASYNC SALES ORDER EVENT HANDLER
# Location: tally_connect/tally_integration/doctype_handlers/sales_order.py
# Action: CREATE this NEW file
# ================================================================================

"""
Sales Order event handlers for Tally integration.
All Tally operations happen in background - don't block user.
"""

import frappe
from frappe import _


def on_submit(doc, method=None):
    """
    Called when Sales Order is submitted.
    Enqueue background job to check/create masters in Tally.

    User doesn't wait - SO submission is instant.
    """
    # Only process if Tally integration is enabled
    try:
        settings = frappe.get_single('Tally Integration Settings')
        if not settings.enabled:
            return
    except Exception:
        return  # Settings not configured

    # Check if this SO should be synced
    if not should_sync_sales_order(doc):
        return

    # ENQUEUE BACKGROUND JOB - Don't block user!
    frappe.enqueue(
        method='tally_connect.tally_integration.doctype_handlers.sales_order.process_sales_order_masters',
        queue='short',  # Use short queue for quick operations
        timeout=300,  # 5 minute timeout
        is_async=True,  # Run in background
        now=False,  # Don't run immediately in current request
        job_name=f'Tally Masters for SO-{doc.name}',
        doc_name=doc.name,
        company=doc.company
    )

    # Show user message
    frappe.msgprint(
        _('Sales Order submitted. Tally sync queued in background.'),
        indicator='blue',
        alert=True
    )


def should_sync_sales_order(doc):
    """
    Check if this Sales Order should trigger Tally master creation.
    """
    # Add your business logic here
    # Example: Only sync if certain criteria met

    # For now, sync all submitted SOs
    return doc.docstatus == 1


def process_sales_order_masters(doc_name, company):
    """
    Background job: Check and create all required masters in Tally.
    This runs AFTER user submission completes.

    Process:
    1. Check customer exists in Tally
    2. Check all items exist in Tally
    3. If missing, trigger creation (which will retry if fails)
    """
    try:
        doc = frappe.get_doc('Sales Order', doc_name)

        # Track what we create
        created_masters = []
        failed_masters = []

        # 1. Check/Create Customer
        customer_result = check_or_create_customer(doc.customer, company)
        if customer_result.get('created') or customer_result.get('retry_scheduled'):
            created_masters.append(f'Customer: {doc.customer}')
        elif not customer_result.get('success') and not customer_result.get('already_exists'):
            failed_masters.append(f'Customer: {doc.customer}')

        # 2. Check/Create Items
        for item in doc.items:
            item_result = check_or_create_stock_item(item.item_code, company)
            if item_result.get('created') or item_result.get('retry_scheduled'):
                created_masters.append(f'Item: {item.item_code}')
            elif not item_result.get('success') and not item_result.get('already_exists'):
                failed_masters.append(f'Item: {item.item_code}')

        # Log results
        if created_masters:
            frappe.logger().info(
                f'Tally masters created/queued for SO {doc_name}: {created_masters}'
            )

        if failed_masters:
            frappe.logger().error(
                f'Tally master creation failed for SO {doc_name}: {failed_masters}'
            )

    except Exception as e:
        frappe.log_error(
            f'Error processing Tally masters for SO {doc_name}: {str(e)}',
            'Tally SO Master Processing'
        )


def check_or_create_customer(customer_name, company):
    """
    Check if customer exists in Tally, create if missing.
    Returns immediately - doesn't wait for Tally.
    """
    from tally_connect.tally_integration.api.creators import (
        create_customer_ledger_in_tally
    )

    try:
        result = create_customer_ledger_in_tally(
            customer_name=customer_name,
            company=company
        )

        if result.get('retry_scheduled'):
            result['created'] = True  # Mark as handled

        return result
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


def check_or_create_stock_item(item_code, company):
    """
    Check if stock item exists in Tally, create if missing.
    Returns immediately - doesn't wait for Tally.
    """
    from tally_connect.tally_integration.api.creators import (
        create_stock_item_in_tally
    )

    try:
        result = create_stock_item_in_tally(
            item_code=item_code,
            company=company
        )

        if result.get('retry_scheduled'):
            result['created'] = True  # Mark as handled

        return result
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
