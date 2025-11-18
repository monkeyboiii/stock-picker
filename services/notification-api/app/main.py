"""
Notification API - Email, SMS, and Push Notification Service

FastAPI service for:
- Email notifications (SendGrid integration)
- SMS notifications (Twilio integration)
- Push notifications (FCM integration)
- Template-based messaging
"""

from contextlib import asynccontextmanager
from enum import Enum
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from loguru import logger
from pydantic import BaseModel, EmailStr, Field

from app.providers.email import EmailProvider
from app.providers.push import PushProvider
from app.providers.sms import SMSProvider


class NotificationType(str, Enum):
    """Notification types"""

    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"


class NotificationStatus(str, Enum):
    """Notification delivery status"""

    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


# Pydantic schemas
class EmailRequest(BaseModel):
    """Email notification request"""

    to: List[EmailStr] = Field(..., description="Recipient email addresses")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body (HTML or plain text)")
    from_email: Optional[EmailStr] = Field(None, description="Sender email (optional)")
    template_id: Optional[str] = Field(None, description="Template ID (optional)")
    template_data: Optional[Dict] = Field(None, description="Template variables")


class SMSRequest(BaseModel):
    """SMS notification request"""

    to: List[str] = Field(..., description="Recipient phone numbers (E.164 format)")
    message: str = Field(..., max_length=1600, description="SMS message")
    template_id: Optional[str] = Field(None, description="Template ID (optional)")
    template_data: Optional[Dict] = Field(None, description="Template variables")


class PushRequest(BaseModel):
    """Push notification request"""

    tokens: List[str] = Field(..., description="Device FCM tokens")
    title: str = Field(..., description="Notification title")
    body: str = Field(..., description="Notification body")
    data: Optional[Dict] = Field(None, description="Additional data payload")
    template_id: Optional[str] = Field(None, description="Template ID (optional)")
    template_data: Optional[Dict] = Field(None, description="Template variables")


class NotificationResponse(BaseModel):
    """Notification response"""

    success: bool
    notification_type: NotificationType
    recipients_count: int
    message: str
    message_ids: Optional[List[str]] = None


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    version: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle handler for FastAPI app"""
    logger.info("Starting Notification API...")
    yield
    logger.info("Shutting down Notification API...")


# FastAPI app
app = FastAPI(
    title="Notification API",
    description="Email, SMS, and push notification service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Initialize providers
email_provider = EmailProvider()
sms_provider = SMSProvider()
push_provider = PushProvider()


@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return HealthResponse(status="healthy", version="1.0.0")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint"""
    return HealthResponse(status="healthy", version="1.0.0")


@app.post("/email", response_model=NotificationResponse)
async def send_email(request: EmailRequest):
    """
    Send email notification

    - **to**: List of recipient email addresses
    - **subject**: Email subject
    - **body**: Email body (HTML or plain text)
    - **from_email**: Optional sender email (defaults to configured sender)
    - **template_id**: Optional template ID
    - **template_data**: Optional template variables
    """
    try:
        logger.info(f"Sending email to {len(request.to)} recipients...")

        message_ids = await email_provider.send_email(
            to=request.to,
            subject=request.subject,
            body=request.body,
            from_email=request.from_email,
            template_id=request.template_id,
            template_data=request.template_data,
        )

        return NotificationResponse(
            success=True,
            notification_type=NotificationType.EMAIL,
            recipients_count=len(request.to),
            message=f"Email sent to {len(request.to)} recipients",
            message_ids=message_ids,
        )
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send email: {str(e)}",
        )


@app.post("/sms", response_model=NotificationResponse)
async def send_sms(request: SMSRequest):
    """
    Send SMS notification

    - **to**: List of recipient phone numbers (E.164 format, e.g., +1234567890)
    - **message**: SMS message (max 1600 characters)
    - **template_id**: Optional template ID
    - **template_data**: Optional template variables
    """
    try:
        logger.info(f"Sending SMS to {len(request.to)} recipients...")

        message_ids = await sms_provider.send_sms(
            to=request.to,
            message=request.message,
            template_id=request.template_id,
            template_data=request.template_data,
        )

        return NotificationResponse(
            success=True,
            notification_type=NotificationType.SMS,
            recipients_count=len(request.to),
            message=f"SMS sent to {len(request.to)} recipients",
            message_ids=message_ids,
        )
    except Exception as e:
        logger.error(f"Failed to send SMS: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send SMS: {str(e)}",
        )


@app.post("/push", response_model=NotificationResponse)
async def send_push(request: PushRequest):
    """
    Send push notification

    - **tokens**: List of device FCM tokens
    - **title**: Notification title
    - **body**: Notification body
    - **data**: Optional additional data payload
    - **template_id**: Optional template ID
    - **template_data**: Optional template variables
    """
    try:
        logger.info(f"Sending push notification to {len(request.tokens)} devices...")

        message_ids = await push_provider.send_push(
            tokens=request.tokens,
            title=request.title,
            body=request.body,
            data=request.data,
            template_id=request.template_id,
            template_data=request.template_data,
        )

        return NotificationResponse(
            success=True,
            notification_type=NotificationType.PUSH,
            recipients_count=len(request.tokens),
            message=f"Push notification sent to {len(request.tokens)} devices",
            message_ids=message_ids,
        )
    except Exception as e:
        logger.error(f"Failed to send push notification: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send push notification: {str(e)}",
        )


@app.get("/templates")
async def list_templates():
    """
    List available notification templates

    Returns a list of available templates for email, SMS, and push notifications
    """
    return {
        "email_templates": email_provider.list_templates(),
        "sms_templates": sms_provider.list_templates(),
        "push_templates": push_provider.list_templates(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8004)
