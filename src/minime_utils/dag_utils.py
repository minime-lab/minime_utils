"""Utilities for jobs executed by DAG operators; no Airflow dependency required."""

from __future__ import annotations

import logging
import os
import sys
import traceback

logger = logging.getLogger(__name__)


def notify_failure(
    job_name: str, dag_name: str, run_id: str, exit_code: int, error_message: str = ""
) -> None:
    """Email a job failure, suppressing delivery errors.

    Call inside an exception handler to include its active traceback. Otherwise
    provide ``error_message`` or use the exit-code fallback. Credentials and the
    recipient come from the job's environment; retry policy is caller-managed.
    """
    error = sys.exception()
    details = (
        "".join(traceback.format_exception(error))
        if error is not None
        else error_message
        or f"Job returned exit code {exit_code}; see the job logs for details."
    )
    try:
        from minime_utils.email import EmailClient

        EmailClient.from_env().send_email(
            title=f"Job failed: {dag_name} / {job_name}",
            message=(
                f"DAG: {dag_name}\nJob: {job_name}\nRun: {run_id}\n"
                f"Exit code: {exit_code}\n\nError:\n{details}"
            ),
            receiver_email=os.environ["DAG_FAILURE_EMAIL"],
        )
    except Exception:
        logger.exception("Could not send failure email for %s", job_name)
