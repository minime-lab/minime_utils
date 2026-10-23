"""Tests for Gmail SMTP email sending."""

from email import message_from_bytes
import smtplib
from unittest.mock import MagicMock, patch

import pytest

from minime_utils.email import EmailClient


def test_from_env_reads_and_strips_credentials(monkeypatch):
    monkeypatch.setenv("GMAIL_ADDRESS", "  alerts@gmail.com ")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "  app-password ")

    client = EmailClient.from_env()

    assert client.email == "alerts@gmail.com"
    assert client.password == "app-password"


def test_from_env_raises_for_missing_credentials(monkeypatch):
    monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("GMAIL_APP_PASSWORD", raising=False)

    with pytest.raises(ValueError) as exc_info:
        EmailClient.from_env()

    assert "GMAIL_ADDRESS" in str(exc_info.value)
    assert "GMAIL_APP_PASSWORD" in str(exc_info.value)


def test_send_email_uses_gmail_ssl_and_sends_plain_text():
    smtp = MagicMock()
    smtp_context = MagicMock()
    smtp_context.__enter__.return_value = smtp

    with patch(
        "minime_utils.email.smtplib.SMTP_SSL", return_value=smtp_context
    ) as connect:
        EmailClient("alerts@gmail.com", "secret").send_email(
            "Alert title", "Alert body", "me@example.com"
        )

    connect.assert_called_once()
    assert connect.call_args.args[:2] == ("smtp.gmail.com", 465)
    assert connect.call_args.kwargs["context"] is not None
    smtp.login.assert_called_once_with("alerts@gmail.com", "secret")
    sent = smtp.send_message.call_args.args[0]
    assert sent["Subject"] == "Alert title"
    assert sent["From"] == "alerts@gmail.com"
    assert sent["To"] == "me@example.com"
    assert message_from_bytes(sent.as_bytes()).get_content().strip() == "Alert body"


def test_send_email_propagates_smtp_errors():
    smtp_context = MagicMock()
    smtp_context.__enter__.side_effect = smtplib.SMTPAuthenticationError(535, b"denied")

    with (
        patch("minime_utils.email.smtplib.SMTP_SSL", return_value=smtp_context),
        pytest.raises(smtplib.SMTPAuthenticationError),
    ):
        EmailClient("alerts@gmail.com", "bad-secret").send_email(
            "Alert", "Body", "me@example.com"
        )
