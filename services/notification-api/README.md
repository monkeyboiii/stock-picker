# Notification API

Email, SMS, and push notification service

## Overview

Notification API is a FastAPI-based microservice that provides:

- **Email Notifications**: SendGrid integration (stub)
- **SMS Notifications**: Twilio integration (stub)
- **Push Notifications**: Firebase Cloud Messaging integration (stub)
- **Template Support**: Pre-defined message templates
- **Multi-Channel**: Send notifications across multiple channels

## Tech Stack

- **Framework**: FastAPI 0.115+ (async Python web framework)
- **Providers** (optional dependencies):
  - SendGrid for email
  - Twilio for SMS
  - Firebase Cloud Messaging for push notifications
- **Python**: 3.13+

## API Endpoints

### Notification Endpoints

- `POST /email` - Send email notification
  - Parameters: `to` (list), `subject`, `body`, `from_email` (optional), `template_id` (optional)
  - Returns: Success status, message IDs

- `POST /sms` - Send SMS notification
  - Parameters: `to` (list), `message`, `template_id` (optional)
  - Returns: Success status, message IDs

- `POST /push` - Send push notification
  - Parameters: `tokens` (list), `title`, `body`, `data` (optional), `template_id` (optional)
  - Returns: Success status, message IDs

### Template Management

- `GET /templates` - List available templates
  - Returns: Email, SMS, and push notification templates

### Health Checks

- `GET /` - Basic health check
- `GET /health` - Health check

## Development

### Prerequisites

- Python 3.13+
- uv (Python package manager)

### Setup

```bash
# Install dependencies (base only - no provider integrations)
cd services/notification-api
uv sync

# To install with real provider integrations:
uv sync --extra providers

# Set environment variables (if using real providers)
export SENDGRID_API_KEY=your-sendgrid-key
export TWILIO_ACCOUNT_SID=your-twilio-sid
export TWILIO_AUTH_TOKEN=your-twilio-token
export FCM_CREDENTIALS_PATH=/path/to/fcm-credentials.json

# Run the service
uv run uvicorn app.main:app --reload --port 8004
```

### Testing

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=html
```

### Docker

```bash
# Build image
docker build -t notification-api:latest .

# Run container
docker run -p 8004:8004 \
  -e SENDGRID_API_KEY=your-key \
  -e TWILIO_ACCOUNT_SID=your-sid \
  -e TWILIO_AUTH_TOKEN=your-token \
  notification-api:latest
```

## API Documentation

Once running, visit:

- **Swagger UI**: http://localhost:8004/docs
- **ReDoc**: http://localhost:8004/redoc
- **OpenAPI JSON**: http://localhost:8004/openapi.json

## Provider Integration

### Current Status

This is a **stub implementation** with logging only. To enable real notifications:

1. Install provider dependencies:
   ```bash
   uv sync --extra providers
   ```

2. Configure API keys in environment variables

3. Uncomment provider integration code in:
   - `app/providers/email.py` (SendGrid)
   - `app/providers/sms.py` (Twilio)
   - `app/providers/push.py` (Firebase)

### Email (SendGrid)

```python
# Uncomment in email.py:
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

message = Mail(
    from_email=sender,
    to_emails=to,
    subject=subject,
    html_content=body
)
sg = SendGridAPIClient(self.api_key)
response = sg.send(message)
```

### SMS (Twilio)

```python
# Uncomment in sms.py:
from twilio.rest import Client

client = Client(self.account_sid, self.auth_token)
message = client.messages.create(
    to=recipient,
    from_=self.from_number,
    body=message
)
```

### Push (Firebase Cloud Messaging)

```python
# Uncomment in push.py:
import firebase_admin
from firebase_admin import credentials, messaging

cred = credentials.Certificate(self.credentials_path)
firebase_admin.initialize_app(cred)

message = messaging.Message(
    notification=messaging.Notification(title=title, body=body),
    token=token,
)
response = messaging.send(message)
```

## Environment Variables

Optional (for real provider integration):
- `SENDGRID_API_KEY` - SendGrid API key
- `SENDGRID_FROM_EMAIL` - Default sender email (default: noreply@stockpicker.com)
- `TWILIO_ACCOUNT_SID` - Twilio account SID
- `TWILIO_AUTH_TOKEN` - Twilio auth token
- `TWILIO_FROM_NUMBER` - Twilio phone number (E.164 format)
- `FCM_CREDENTIALS_PATH` - Path to Firebase credentials JSON

## Available Templates

### Email Templates
- **welcome_email** - Sent when user registers
- **password_reset** - Sent when user requests password reset
- **backtest_complete** - Sent when backtest finishes

### SMS Templates
- **verification_code** - Sent for 2FA verification
- **price_alert** - Sent when stock hits target price

### Push Notification Templates
- **trade_executed** - Sent when a trade is executed
- **backtest_ready** - Sent when backtest results are ready
- **market_alert** - Sent for market alerts

## Usage Examples

### Send Email

```bash
curl -X POST http://localhost:8004/email \
  -H "Content-Type: application/json" \
  -d '{
    "to": ["user@example.com"],
    "subject": "Welcome!",
    "body": "<h1>Welcome to Stock Picker</h1>"
  }'
```

### Send SMS

```bash
curl -X POST http://localhost:8004/sms \
  -H "Content-Type: application/json" \
  -d '{
    "to": ["+1234567890"],
    "message": "Your verification code is: 123456"
  }'
```

### Send Push Notification

```bash
curl -X POST http://localhost:8004/push \
  -H "Content-Type: application/json" \
  -d '{
    "tokens": ["device-token-123"],
    "title": "Trade Executed",
    "body": "Your trade for AAPL has been executed",
    "data": {"stock": "AAPL", "quantity": 100}
  }'
```

## Future Enhancements

- [ ] Notification history and logging (database)
- [ ] Delivery status tracking
- [ ] Retry logic for failed notifications
- [ ] Rate limiting and queuing (Celery)
- [ ] Batch notification support
- [ ] Webhook callbacks for delivery status
- [ ] A/B testing for notification content
- [ ] Analytics and reporting

## License

MIT
