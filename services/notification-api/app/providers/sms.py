"""SMS provider using Twilio (stub implementation)"""

from typing import Dict, List, Optional

from loguru import logger


class SMSProvider:
    """
    SMS provider using Twilio

    This is a stub implementation. In production, integrate with:
    - Twilio API: https://www.twilio.com/
    - Or other providers: AWS SNS, Vonage, MessageBird
    """

    def __init__(
        self,
        account_sid: Optional[str] = None,
        auth_token: Optional[str] = None,
        from_number: str = "+1234567890",
    ):
        """
        Initialize SMS provider

        Args:
            account_sid: Twilio account SID (from environment)
            auth_token: Twilio auth token (from environment)
            from_number: Default sender phone number (E.164 format)
        """
        self.account_sid = account_sid or "TWILIO_ACCOUNT_SID_PLACEHOLDER"
        self.auth_token = auth_token or "TWILIO_AUTH_TOKEN_PLACEHOLDER"
        self.from_number = from_number

    async def send_sms(
        self,
        to: List[str],
        message: str,
        template_id: Optional[str] = None,
        template_data: Optional[Dict] = None,
    ) -> List[str]:
        """
        Send SMS to recipients

        Args:
            to: List of recipient phone numbers (E.164 format)
            message: SMS message
            template_id: Optional template ID
            template_data: Optional template variables

        Returns:
            List of message IDs
        """
        logger.info(f"[STUB] Sending SMS from {self.from_number} to {to}")
        logger.info(f"[STUB] Message: {message}")

        if template_id:
            logger.info(f"[STUB] Using template: {template_id}")
            logger.info(f"[STUB] Template data: {template_data}")

        # In production, integrate with Twilio:
        # from twilio.rest import Client
        #
        # client = Client(self.account_sid, self.auth_token)
        # message_ids = []
        # for recipient in to:
        #     message = client.messages.create(
        #         to=recipient,
        #         from_=self.from_number,
        #         body=message
        #     )
        #     message_ids.append(message.sid)
        # return message_ids

        # Stub: Return fake message IDs
        return [f"sms-{i}-stub" for i in range(len(to))]

    def list_templates(self) -> List[Dict]:
        """
        List available SMS templates

        Returns:
            List of template metadata
        """
        return [
            {
                "id": "verification_code",
                "name": "Verification Code",
                "description": "Sent for 2FA verification",
            },
            {
                "id": "price_alert",
                "name": "Price Alert",
                "description": "Sent when stock hits target price",
            },
        ]
