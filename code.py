from msgraph import GraphServiceClient
from azure.identity import InteractiveBrowserCredential
from datetime import datetime, timezone

# Azure App credentials
client_id = "YOUR_CLIENT_ID"
tenant_id = "YOUR_TENANT_ID"

# Auth with interactive browser
credential = InteractiveBrowserCredential(client_id=client_id, tenant_id=tenant_id)
client = GraphServiceClient(credential=credential)

# Get today's UTC date
today = datetime.utcnow().date()

# Get all chats (only accessible chats for signed-in user)
chats = client.me.chats.get().value

for chat in chats:
    chat_id = chat.id
    messages = client.chats.by_chat_id(chat_id).messages.get().value
    
    for msg in messages:
        created_dt = msg.created_date_time.replace(tzinfo=timezone.utc).astimezone()
        if created_dt.date() == today:
            content = msg.body.content.lower()
            if "login" in content or "logout" in content:
                sender = msg.from_.user.display_name
                print(f"{created_dt} | {sender} | {content.strip()}")
