from __future__ import annotations

import sys
import types

import pytest

from minime_utils.dag_utils import notify_failure


@pytest.fixture
def sent_emails(monkeypatch):
    sent = []

    class Client:
        @classmethod
        def from_env(cls):
            return cls()

        def send_email(self, **kwargs):
            sent.append(kwargs)

    module = types.ModuleType("minime_utils.email")
    module.EmailClient = Client
    monkeypatch.setitem(sys.modules, "minime_utils.email", module)
    monkeypatch.setenv("DAG_FAILURE_EMAIL", "recipient@example.invalid")
    return sent


def test_email_includes_original_exception_and_job_identity(sent_emails):
    try:
        raise ValueError("Invalid credentials")
    except ValueError:
        notify_failure("validate_creds", "test_dag.main", "run123", 4)
    assert len(sent_emails) == 1
    email = sent_emails[0]
    assert email["receiver_email"] == "recipient@example.invalid"
    assert "test_dag.main / validate_creds" in email["title"]
    assert "ValueError: Invalid credentials" in email["message"]
    assert "Traceback" in email["message"]
    assert "run123" in email["message"]
    assert "Exit code: 4" in email["message"]


def test_non_exception_failure_has_exit_code(sent_emails):
    notify_failure("validate_creds", "test_dag.main", "run123", 3)
    assert "Job returned exit code 3" in sent_emails[0]["message"]


def test_explicit_error_message_without_exception(sent_emails):
    notify_failure("job", "dag", "run", 3, error_message="Source was not found")
    assert "Source was not found" in sent_emails[0]["message"]


def test_missing_recipient_is_suppressed(monkeypatch, sent_emails):
    monkeypatch.delenv("DAG_FAILURE_EMAIL")
    assert notify_failure("job", "dag", "run", 1) is None
    assert not sent_emails


def test_mail_failure_does_not_replace_original_error(monkeypatch, sent_emails):
    class BrokenClient:
        @classmethod
        def from_env(cls):
            raise RuntimeError("SMTP unavailable")

    monkeypatch.setattr(sys.modules["minime_utils.email"], "EmailClient", BrokenClient)
    try:
        raise ValueError("Original job error")
    except ValueError as original:
        assert notify_failure("job", "dag", "run", 1) is None
        assert sys.exception() is original
    assert not sent_emails
