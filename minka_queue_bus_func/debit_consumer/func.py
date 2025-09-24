import io, json

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        event = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    # Service Connector Hub manda los mensajes en este formato
    records = event.get("messages", [])
    results = []

    for msg in records:
        content = msg.get("content")
        metadata = msg.get("metadata", {})
        channel = metadata.get("channelId", "unknown")

        # Decodifica el mensaje publicado por la función encoladora
        try:
            payload = json.loads(content)
        except:
            payload = content

        print(f"=== Mensaje recibido ===")
        print(f"Channel: {channel}")
        print(f"Payload: {payload}")

        # Aquí defines la lógica según el canal
        if channel == "Prepared":
            # TODO: lógica de prepared
            results.append({"status": "ok", "channel": channel, "id": msg.get("id")})
        elif channel == "Aborted":
            # TODO: lógica de aborted
            results.append({"status": "ok", "channel": channel, "id": msg.get("id")})
        elif channel == "Committed":
            # TODO: lógica de committed
            results.append({"status": "ok", "channel": channel, "id": msg.get("id")})
        elif channel == "Completed":
            # TODO: lógica de completed
            results.append({"status": "ok", "channel": channel, "id": msg.get("id")})
        else:
            results.append({"status": "ignored", "channel": channel})

    return (200, json.dumps({"processed": results}))
