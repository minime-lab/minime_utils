"""Send plain-text email through Gmail's SMTP service."""

from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import os
import smtplib
import ssl


@dataclass
class EmailClient:
    """A small Gmail SMTP client configured with an app password."""

    email: str
    password: str

    @classmethod
    def from_env(cls) -> EmailClient:
        """Create a client from ``GMAIL_ADDRESS`` and ``GMAIL_APP_PASSWORD``."""
        email = os.environ.get("GMAIL_ADDRESS", "").strip()
        password = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
        missing = [
            name
            for name, value in (
                ("GMAIL_ADDRESS", email),
                ("GMAIL_APP_PASSWORD", password),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"Missing required email environment variables: {', '.join(missing)}"
            )
        return cls(email=email, password=password)

    def send_email(self, title: str, message: str, receiver_email: str) -> None:
        """Send a plain-text email over implicit TLS; SMTP errors propagate."""
        email_message = EmailMessage()
        email_message["Subject"] = title
        email_message["From"] = self.email
        email_message["To"] = receiver_email
        email_message.set_content(message)

        with smtplib.SMTP_SSL(
            "smtp.gmail.com", 465, context=ssl.create_default_context()
        ) as smtp:
            smtp.login(self.email, self.password)
            smtp.send_message(email_message)
