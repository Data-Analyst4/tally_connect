# ================================================================================
# FILE 4: retry_scheduler.py - BACKGROUND RETRY PROCESSOR
# Location: tally_connect/tally_integration/tasks/retry_scheduler.py
# Action: CREATE this NEW file
# ================================================================================

"""
Scheduled task to process pending retry jobs.
Runs every 1 minute via Frappe scheduler.
"""

import frappe
from datetime import datetime


def process_pending_retries():
    """
    Pick up PENDING retry jobs that are due and execute them.
    Runs every 1 minute.
    """
    now = datetime.now()

    pending_jobs = frappe.get_all(
        'Tally Retry Job',
        filters={
            'status': 'PENDING',
            'scheduled_at': ['<=', now]
        },
        fields=['name', 'document_type', 'document_name', 'attempt_number'],
        order_by='scheduled_at asc',
        limit_page_length=20  # Process 20 at a time
    )

    if not pending_jobs:
        return

    frappe.logger().info(f'Processing {len(pending_jobs)} pending Tally retry jobs')

    for job_data in pending_jobs:
        try:
            execute_retry_job(job_data)
        except Exception as e:
            frappe.logger().error(
                f'Retry job execution failed: {job_data["name"]} - {str(e)}'
            )


def execute_retry_job(job_data):
    """
    Execute a single retry job.
    """
    job = frappe.get_doc('Tally Retry Job', job_data['name'])

    # Update status
    job.status = 'IN_PROGRESS'
    job.save(ignore_permissions=True)
    frappe.db.commit()

    try:
        result = None

        # Route based on document type
        if job.document_type == 'Item':
            from tally_connect.tally_integration.api.creators import (
                create_stock_item_in_tally
            )
            result = create_stock_item_in_tally(
                item_code=job.document_name,
                is_retry=True,
                retry_count=job.attempt_number
            )

        elif job.document_type == 'Customer':
            from tally_connect.tally_integration.api.creators import (
                create_customer_ledger_in_tally
            )
            result = create_customer_ledger_in_tally(
                customer_name=job.document_name,
                is_retry=True,
                retry_count=job.attempt_number
            )

        elif job.document_type == 'Supplier':
            from tally_connect.tally_integration.api.creators import (
                create_supplier_ledger_in_tally
            )
            result = create_supplier_ledger_in_tally(
                supplier_name=job.document_name,
                is_retry=True,
                retry_count=job.attempt_number
            )

        # Check result
        if result and result.get('success'):
            job.status = 'SUCCESS'
            frappe.logger().info(
                f'Retry job {job.name} succeeded on attempt {job.attempt_number}'
            )
        else:
            # Check if another retry was scheduled
            if result and result.get('retry_scheduled'):
                job.status = 'PENDING'  # Will be picked up by next scheduler run
            else:
                job.status = 'FAILED'
                frappe.logger().warning(
                    f'Retry job {job.name} failed after {job.attempt_number} attempts'
                )

    except Exception as e:
        job.status = 'FAILED'
        job.error_message = str(e)[:500]
        frappe.logger().error(f'Retry job {job.name} error: {str(e)}')

    job.save(ignore_permissions=True)
    frappe.db.commit()
