"""Email provider using SendGrid (stub implementation)"""

from typing import Dict, List, Optional

from loguru import logger


class EmailProvider:
    """
    Email provider using SendGrid

    This is a stub implementation. In production, integrate with:
    - SendGrid API: https://sendgrid.com/
    - Or other providers: AWS SES, Mailgun, Postmark
    """

    def __init__(self, api_key: Optional[str] = None, from_email: str = "noreply@stockpicker.com"):
        """
        Initialize email provider

        Args:
            api_key: SendGrid API key (from environment)
            from_email: Default sender email
        """
        self.api_key = api_key or "SENDGRID_API_KEY_PLACEHOLDER"
        self.from_email = from_email

    async def send_email(
        self,
        to: List[str],
        subject: str,
        body: str,
        from_email: Optional[str] = None,
        template_id: Optional[str] = None,
        template_data: Optional[Dict] = None,
    ) -> List[str]:
        """
        Send email to recipients

        Args:
            to: List of recipient emails
            subject: Email subject
            body: Email body (HTML or plain text)
            from_email: Optional sender email
            template_id: Optional template ID
            template_data: Optional template variables

        Returns:
            List of message IDs
        """
        sender = from_email or self.from_email

        logger.info(f"[STUB] Sending email from {sender} to {to}")
        logger.info(f"[STUB] Subject: {subject}")
        logger.info(f"[STUB] Body length: {len(body)} characters")

        if template_id:
            logger.info(f"[STUB] Using template: {template_id}")
            logger.info(f"[STUB] Template data: {template_data}")

        # In production, integrate with SendGrid:
        # from sendgrid import SendGridAPIClient
        # from sendgrid.helpers.mail import Mail
        #
        # message = Mail(
        #     from_email=sender,
        #     to_emails=to,
        #     subject=subject,
        #     html_content=body
        # )
        # sg = SendGridAPIClient(self.api_key)
        # response = sg.send(message)
        # return [response.headers.get('X-Message-Id')]

        # Stub: Return fake message IDs
        return [f"email-{i}-stub" for i in range(len(to))]

    def list_templates(self) -> List[Dict]:
        """
        List available email templates

        Returns:
            List of template metadata
        """
        return [
            {
                "id": "welcome_email",
                "name": "Welcome Email",
                "description": "Sent when user registers",
            },
            {
                "id": "password_reset",
                "name": "Password Reset",
                "description": "Sent when user requests password reset",
            },
            {
                "id": "backtest_complete",
                "name": "Backtest Complete",
                "description": "Sent when backtest finishes",
            },
        ]
