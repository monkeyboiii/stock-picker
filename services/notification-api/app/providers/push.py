"""Push notification provider using Firebase Cloud Messaging (stub implementation)"""

from typing import Dict, List, Optional

from loguru import logger


class PushProvider:
    """
    Push notification provider using Firebase Cloud Messaging (FCM)

    This is a stub implementation. In production, integrate with:
    - Firebase Cloud Messaging: https://firebase.google.com/docs/cloud-messaging
    - Or other providers: APNs (iOS), OneSignal, Pusher
    """

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize push notification provider

        Args:
            credentials_path: Path to FCM credentials JSON file (from environment)
        """
        self.credentials_path = credentials_path or "FCM_CREDENTIALS_PATH_PLACEHOLDER"

    async def send_push(
        self,
        tokens: List[str],
        title: str,
        body: str,
        data: Optional[Dict] = None,
        template_id: Optional[str] = None,
        template_data: Optional[Dict] = None,
    ) -> List[str]:
        """
        Send push notification to devices

        Args:
            tokens: List of device FCM tokens
            title: Notification title
            body: Notification body
            data: Optional additional data payload
            template_id: Optional template ID
            template_data: Optional template variables

        Returns:
            List of message IDs
        """
        logger.info(f"[STUB] Sending push notification to {len(tokens)} devices")
        logger.info(f"[STUB] Title: {title}")
        logger.info(f"[STUB] Body: {body}")
        logger.info(f"[STUB] Data: {data}")

        if template_id:
            logger.info(f"[STUB] Using template: {template_id}")
            logger.info(f"[STUB] Template data: {template_data}")

        # In production, integrate with Firebase Admin SDK:
        # import firebase_admin
        # from firebase_admin import credentials, messaging
        #
        # if not firebase_admin._apps:
        #     cred = credentials.Certificate(self.credentials_path)
        #     firebase_admin.initialize_app(cred)
        #
        # messages = [
        #     messaging.Message(
        #         notification=messaging.Notification(title=title, body=body),
        #         data=data or {},
        #         token=token,
        #     )
        #     for token in tokens
        # ]
        #
        # response = messaging.send_all(messages)
        # return [msg.message_id for msg in response.responses if msg.success]

        # Stub: Return fake message IDs
        return [f"push-{i}-stub" for i in range(len(tokens))]

    def list_templates(self) -> List[Dict]:
        """
        List available push notification templates

        Returns:
            List of template metadata
        """
        return [
            {
                "id": "trade_executed",
                "name": "Trade Executed",
                "description": "Sent when a trade is executed",
            },
            {
                "id": "backtest_ready",
                "name": "Backtest Ready",
                "description": "Sent when backtest results are ready",
            },
            {
                "id": "market_alert",
                "name": "Market Alert",
                "description": "Sent for market alerts",
            },
        ]
