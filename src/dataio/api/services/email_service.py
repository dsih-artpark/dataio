"""
Email service for sending OTP codes and notifications.

Supports two email providers:
1. AWS SES (default) - Uses boto3, credentials from environment/IAM
2. SMTP - Generic SMTP server (SendGrid, Mailgun, etc.)

Set EMAIL_PROVIDER=smtp to use SMTP instead of SES.
Set DEBUG_EMAIL=true to print emails to console/logs instead of sending.
This is useful for local development and testing without email configuration.
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

from dataio.api.services.base_service import BaseService

logger = logging.getLogger(__name__)

# Debug mode - prints emails to console instead of sending
# Set DEBUG_EMAIL=true in .env for local development
DEBUG_EMAIL = os.getenv("DEBUG_EMAIL", "false").lower() == "true"

# Email provider: "ses" (default) or "smtp"
EMAIL_PROVIDER = os.getenv("EMAIL_PROVIDER", "ses").lower()

# Common configuration
EMAIL_FROM_ADDRESS = os.getenv("EMAIL_FROM_ADDRESS", "noreply@dataio.artpark.ai")
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "DataIO")

# AWS SES Configuration
AWS_SES_REGION = os.getenv("AWS_SES_REGION", "ap-south-1")

# SMTP Configuration (fallback if EMAIL_PROVIDER=smtp)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.sendgrid.net")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "apikey")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"


class EmailService(BaseService):
    """Service for sending emails via AWS SES, SMTP, or debug console."""

    def __init__(self):
        super().__init__()
        self.debug_mode = DEBUG_EMAIL
        self.provider = EMAIL_PROVIDER
        self.from_email = EMAIL_FROM_ADDRESS
        self.from_name = EMAIL_FROM_NAME

        # SES configuration
        self.ses_region = AWS_SES_REGION
        self._ses_client = None

        # SMTP configuration
        self.smtp_host = SMTP_HOST
        self.smtp_port = SMTP_PORT
        self.smtp_user = SMTP_USER
        self.smtp_password = SMTP_PASSWORD
        self.smtp_use_tls = SMTP_USE_TLS

        if self.debug_mode:
            self.logger.warning(
                "EMAIL DEBUG MODE ENABLED - Emails will be printed to console, not sent"
            )
        else:
            self.logger.info(f"Email provider: {self.provider.upper()}")

    @property
    def ses_client(self):
        """Lazy-load SES client to avoid import errors if not using SES."""
        if self._ses_client is None:
            import boto3

            self._ses_client = boto3.client("ses", region_name=self.ses_region)
        return self._ses_client

    def _create_smtp_connection(self) -> smtplib.SMTP:
        """Create and authenticate SMTP connection."""
        server = smtplib.SMTP(self.smtp_host, self.smtp_port)
        if self.smtp_use_tls:
            server.starttls()
        if self.smtp_user and self.smtp_password:
            server.login(self.smtp_user, self.smtp_password)
        return server

    def _debug_print_email(
        self,
        to_email: str,
        subject: str,
        text_body: Optional[str] = None,
    ) -> None:
        """Print email to console/logs for debugging."""
        separator = "=" * 60
        self.logger.info(f"\n{separator}")
        self.logger.info("DEBUG EMAIL (not actually sent)")
        self.logger.info(separator)
        self.logger.info(f"To: {to_email}")
        self.logger.info(f"From: {self.from_name} <{self.from_email}>")
        self.logger.info(f"Subject: {subject}")
        self.logger.info(separator)
        if text_body:
            self.logger.info(text_body)
        self.logger.info(f"{separator}\n")

        # Also print to stdout for visibility in console
        print(f"\n{separator}")
        print("DEBUG EMAIL (not actually sent)")
        print(separator)
        print(f"To: {to_email}")
        print(f"From: {self.from_name} <{self.from_email}>")
        print(f"Subject: {subject}")
        print(separator)
        if text_body:
            print(text_body)
        print(f"{separator}\n")

    def _send_via_ses(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None,
    ) -> bool:
        """Send email via AWS SES."""
        try:
            body = {"Html": {"Charset": "UTF-8", "Data": html_body}}
            if text_body:
                body["Text"] = {"Charset": "UTF-8", "Data": text_body}

            response = self.ses_client.send_email(
                Source=f"{self.from_name} <{self.from_email}>",
                Destination={"ToAddresses": [to_email]},
                Message={
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                    "Body": body,
                },
            )
            self.logger.info(
                f"Email sent via SES to: {to_email} (MessageId: {response['MessageId']})"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to send email via SES to {to_email}: {str(e)}")
            return False

    def _send_via_smtp(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None,
    ) -> bool:
        """Send email via SMTP server."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{self.from_name} <{self.from_email}>"
            msg["To"] = to_email

            # Add plain text version if provided
            if text_body:
                msg.attach(MIMEText(text_body, "plain"))

            # Add HTML version
            msg.attach(MIMEText(html_body, "html"))

            # Send email
            server = self._create_smtp_connection()
            try:
                server.sendmail(self.from_email, to_email, msg.as_string())
                self.logger.info(f"Email sent via SMTP to: {to_email}")
                return True
            finally:
                server.quit()

        except Exception as e:
            self.logger.error(f"Failed to send email via SMTP to {to_email}: {str(e)}")
            return False

    def send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None,
    ) -> bool:
        """
        Send an email (or print to console in debug mode).

        Args:
            to_email: Recipient email address
            subject: Email subject
            html_body: HTML content of the email
            text_body: Optional plain text content

        Returns:
            bool: True if sent successfully (always True in debug mode)
        """
        # Debug mode - just print to console
        if self.debug_mode:
            self._debug_print_email(to_email, subject, text_body)
            return True

        # Production mode - send via configured provider
        if self.provider == "ses":
            return self._send_via_ses(to_email, subject, html_body, text_body)
        else:
            return self._send_via_smtp(to_email, subject, html_body, text_body)

    def send_otp_email(self, to_email: str, otp_code: str) -> bool:
        """
        Send an OTP code email for login.

        Args:
            to_email: Recipient email address
            otp_code: The OTP code

        Returns:
            bool: True if sent successfully
        """
        subject = "Your DataIO login code"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">Your login code</h2>
        <p>Use the following code to complete your login:</p>
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; text-align: center; margin: 20px 0;">
            <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #667eea;">{otp_code}</span>
        </div>
        <p style="color: #666; font-size: 14px;">
            This code will expire in 10 minutes. If you didn't request this code,
            you can safely ignore this email.
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
DataIO - Your login code

Use the following code to complete your login:

{otp_code}

This code will expire in 10 minutes.

If you didn't request this code, you can safely ignore this email.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_invite_email(
        self,
        to_email: str,
        invitation_link: str,
        inviter_name: Optional[str] = None,
    ) -> bool:
        """
        Send an invitation email to a new user with a magic link.

        Args:
            to_email: Recipient email address
            invitation_link: The magic link URL for accepting the invitation
            inviter_name: Optional name of the person who invited them

        Returns:
            bool: True if sent successfully
        """
        inviter_text = f" by {inviter_name}" if inviter_name else ""
        subject = "You've been invited to DataIO"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">Welcome to DataIO!</h2>
        <p>You've been invited{inviter_text} to join DataIO, a dataset management platform.</p>
        <p>Click the button below to accept your invitation and set up your account:</p>
        <div style="text-align: center; margin: 25px 0;">
            <a href="{invitation_link}" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Accept Invitation</a>
        </div>
        <p style="color: #666; font-size: 14px;">
            This invitation will expire in 48 hours. After accepting, you'll be prompted
            to set up a passkey for secure, passwordless access.
        </p>
        <p style="color: #999; font-size: 12px;">
            If the button doesn't work, copy and paste this link into your browser:<br>
            <a href="{invitation_link}" style="color: #667eea; word-break: break-all;">{invitation_link}</a>
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
Welcome to DataIO!

You've been invited{inviter_text} to join DataIO, a dataset management platform.

Click the link below to accept your invitation and set up your account:

{invitation_link}

This invitation will expire in 48 hours.

After accepting, you'll be prompted to set up a passkey for secure, passwordless access.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_passkey_added_email(
        self,
        to_email: str,
        device_name: str,
    ) -> bool:
        """
        Send a notification when a new passkey is added.

        Args:
            to_email: Recipient email address
            device_name: Name of the device/passkey

        Returns:
            bool: True if sent successfully
        """
        subject = "New passkey added to your DataIO account"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">New passkey added</h2>
        <p>A new passkey has been added to your DataIO account:</p>
        <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; margin: 20px 0;">
            <strong>{device_name}</strong>
        </div>
        <p style="color: #666; font-size: 14px;">
            If you did not add this passkey, please contact support immediately
            and remove any unauthorized passkeys from your account settings.
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
New passkey added to your DataIO account

A new passkey has been added to your account:

{device_name}

If you did not add this passkey, please contact support immediately and remove any unauthorized passkeys from your account settings.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_api_key_created_email(
        self,
        to_email: str,
        key_name: str,
    ) -> bool:
        """
        Send a notification when a new API key is created.

        Args:
            to_email: Recipient email address
            key_name: Name of the API key

        Returns:
            bool: True if sent successfully
        """
        subject = "New API key created for your DataIO account"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">New API key created</h2>
        <p>A new API key has been created for your DataIO account:</p>
        <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; margin: 20px 0;">
            <strong>{key_name}</strong>
        </div>
        <p style="color: #666; font-size: 14px;">
            If you did not create this API key, please revoke it immediately
            from your account settings.
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
New API key created for your DataIO account

A new API key has been created for your account:

{key_name}

If you did not create this API key, please revoke it immediately from your account settings.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_sign_in_alert_email(
        self,
        to_email: str,
        method: str,
        timestamp: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> bool:
        """Send a sign-in notification email for a successful interactive login."""
        subject = "New sign-in to your DataIO account"
        ip_text = ip_address or "Unavailable"
        user_agent_text = user_agent or "Unavailable"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">New sign-in detected</h2>
        <p>Your DataIO account was just accessed successfully.</p>
        <div style="background: #f5f5f5; padding: 15px; border-radius: 8px; margin: 20px 0;">
            <p style="margin: 0 0 8px 0;"><strong>Method:</strong> {method}</p>
            <p style="margin: 0 0 8px 0;"><strong>Time:</strong> {timestamp}</p>
            <p style="margin: 0 0 8px 0;"><strong>IP address:</strong> {ip_text}</p>
            <p style="margin: 0;"><strong>Browser/device:</strong> {user_agent_text}</p>
        </div>
        <p style="color: #666; font-size: 14px;">
            If this was not you, sign out of all sessions and rotate your API keys immediately.
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
New sign-in to your DataIO account

Your DataIO account was accessed successfully.

Method: {method}
Time: {timestamp}
IP address: {ip_text}
Browser/device: {user_agent_text}

If this was not you, sign out of all sessions and rotate your API keys immediately.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_registration_email(
        self,
        to_email: str,
        otp_code: str,
        magic_link: str,
    ) -> bool:
        """
        Send a registration verification email with OTP and magic link.

        Args:
            to_email: Recipient email address
            otp_code: The OTP code
            magic_link: The magic link URL

        Returns:
            bool: True if sent successfully
        """
        subject = "Verify your DataIO registration"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">Verify your email</h2>
        <p>Thanks for registering! Please verify your email to complete your registration.</p>

        <p><strong>Option 1:</strong> Click the button below:</p>
        <div style="text-align: center; margin: 25px 0;">
            <a href="{magic_link}" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Verify Email</a>
        </div>

        <p><strong>Option 2:</strong> Enter this code:</p>
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; text-align: center; margin: 20px 0;">
            <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #667eea;">{otp_code}</span>
        </div>

        <p style="color: #666; font-size: 14px;">
            This verification will expire in 30 minutes. If you didn't register for DataIO,
            you can safely ignore this email.
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
Verify your DataIO registration

Thanks for registering! Please verify your email to complete your registration.

Option 1: Click this link:
{magic_link}

Option 2: Enter this code: {otp_code}

This verification will expire in 30 minutes.

If you didn't register for DataIO, you can safely ignore this email.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_account_deletion_email(
        self,
        to_email: str,
        otp_code: str,
    ) -> bool:
        """
        Send account deletion confirmation email.

        Args:
            to_email: Recipient email address
            otp_code: The OTP code for confirmation

        Returns:
            bool: True if sent successfully
        """
        subject = "Confirm account deletion - DataIO"

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #dc2626; margin-top: 0;">Account Deletion Request</h2>
        <p>You've requested to delete your DataIO account. This action is <strong>permanent</strong> and cannot be undone.</p>

        <div style="background: #fef2f2; border: 1px solid #fecaca; padding: 15px; border-radius: 8px; margin: 20px 0;">
            <p style="color: #dc2626; margin: 0; font-weight: bold;">Warning: This will permanently delete:</p>
            <ul style="color: #991b1b; margin: 10px 0 0 0; padding-left: 20px;">
                <li>Your account and profile</li>
                <li>All your API keys</li>
                <li>All your passkeys</li>
                <li>Your access to all datasets</li>
            </ul>
        </div>

        <p>To confirm deletion, enter this code:</p>
        <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; text-align: center; margin: 20px 0;">
            <span style="font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #dc2626;">{otp_code}</span>
        </div>

        <p style="color: #666; font-size: 14px;">
            This code will expire in 10 minutes. If you did not request account deletion,
            please ignore this email and your account will remain safe.
        </p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = f"""
Account Deletion Request - DataIO

You've requested to delete your DataIO account. This action is PERMANENT and cannot be undone.

WARNING: This will permanently delete:
- Your account and profile
- All your API keys
- All your passkeys
- Your access to all datasets

To confirm deletion, enter this code: {otp_code}

This code will expire in 10 minutes.

If you did not request account deletion, please ignore this email and your account will remain safe.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_verification_approved_email(self, to_email: str) -> bool:
        """
        Send notification that user verification was approved.

        Args:
            to_email: Recipient email address

        Returns:
            bool: True if sent successfully
        """
        subject = "Your DataIO account has been verified"

        html_body = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #059669 0%, #047857 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #059669; margin-top: 0;">Account Verified!</h2>
        <p>Great news! Your DataIO account has been verified by an administrator.</p>
        <p>You now have full access to the platform, including:</p>
        <ul style="color: #333;">
            <li>Browse and download datasets</li>
            <li>Create API keys for programmatic access</li>
            <li>Set up passkeys for secure login</li>
        </ul>
        <div style="text-align: center; margin: 25px 0;">
            <a href="https://dataio.artpark.ai/datasets" style="background: linear-gradient(135deg, #059669 0%, #047857 100%); color: white; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Explore Datasets</a>
        </div>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = """
Account Verified - DataIO

Great news! Your DataIO account has been verified by an administrator.

You now have full access to the platform, including:
- Browse and download datasets
- Create API keys for programmatic access
- Set up passkeys for secure login

Visit https://dataio.artpark.ai/datasets to explore datasets.
"""

        return self.send_email(to_email, subject, html_body, text_body)

    def send_verification_rejected_email(self, to_email: str) -> bool:
        """
        Send notification that user verification was rejected.

        Args:
            to_email: Recipient email address

        Returns:
            bool: True if sent successfully
        """
        subject = "DataIO account verification update"

        html_body = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px 10px 0 0;">
        <h1 style="color: white; margin: 0; font-size: 24px;">DataIO</h1>
    </div>
    <div style="background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px;">
        <h2 style="color: #333; margin-top: 0;">Account Verification Update</h2>
        <p>We were unable to verify your DataIO account at this time.</p>
        <p>DataIO is currently in beta and limited to users from academic and research institutions. If you believe this is an error or would like to request access, please contact us.</p>
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
        <p style="color: #999; font-size: 12px; margin-bottom: 0;">
            This email was sent by DataIO. Please do not reply to this email.
        </p>
    </div>
</body>
</html>
"""

        text_body = """
Account Verification Update - DataIO

We were unable to verify your DataIO account at this time.

DataIO is currently in beta and limited to users from academic and research institutions. If you believe this is an error or would like to request access, please contact us.
"""

        return self.send_email(to_email, subject, html_body, text_body)
