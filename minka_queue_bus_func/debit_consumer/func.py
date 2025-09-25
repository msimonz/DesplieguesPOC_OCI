import io, json

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        event = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    results = []

    # Si es lista, toma el primer elemento (o itera todos)
    if isinstance(event, list):
        events = event
    else:
        events = [event]

    for ev in events:
        for msg in ev.get("messages", []):
            content = msg.get("content") or "{}"
            metadata = msg.get("metadata", {})
            channel = metadata.get("channelId", "unknown")

            try:
                payload = json.loads(content)
            except Exception:
                payload = {"raw_content": content}

            print("=== Mensaje recibido ===")
            print(f"Channel: {channel}")
            print(f"Payload: {payload}")

            results.append({
                "status": "ok",
                "channel": channel,
                "id": msg.get("id"),
                "payload": payload
            })

    return (200, json.dumps({"processed": results}))
