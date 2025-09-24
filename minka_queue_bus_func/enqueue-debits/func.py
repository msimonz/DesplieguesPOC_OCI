import io, json, os
import oci

QUEUE_OCID = os.getenv("QUEUE_OCID")  # OCID de la Queue

def _get_header(headers: dict, name: str):
    if not headers:
        return None
    lower = {k.lower(): v for k, v in headers.items()}
    return lower.get(name.lower())

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        body = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    try:
        headers = ctx.Headers() if hasattr(ctx, "Headers") else {}
        print("=== Headers recibidos ===")
        print(headers)

        channel = _get_header(headers, "x-queue-channel") or body.get("channel")
        if not channel:
            return (400, json.dumps({"error": "Missing channel (x-queue-channel header or body.channel)"}))

        if not QUEUE_OCID:
            return (500, json.dumps({"status": "failed", "reason": "QUEUE_OCID not configured", "channel": channel}))

        signer = oci.auth.signers.get_resource_principals_signer()

        # 1) Descubre el messages endpoint usando el ADMIN client
        admin = oci.queue.QueueAdminClient(config={}, signer=signer)
        q = admin.get_queue(QUEUE_OCID).data  # <- aquí sí existe get_queue
        messages_endpoint = q.messages_endpoint  # <- atributo correcto

        # 2) Crea el data-plane client y apúntalo al messages endpoint de ESTA cola
        queue_client = oci.queue.QueueClient(config={}, signer=signer)
        queue_client.base_client.endpoint = messages_endpoint

        enriched_body = {"payload": body, "channel": channel}
        put_details = oci.queue.models.PutMessagesDetails(
            messages=[
                oci.queue.models.PutMessagesDetailsEntry(
                    content=json.dumps(enriched_body),
                    metadata={"channelId": str(channel)}   # publica al canal
                )
            ]
        )

        resp = queue_client.put_messages(queue_id=QUEUE_OCID, put_messages_details=put_details)
        result = oci.util.to_dict(resp.data)
        print(f"put_messages opc-request-id={resp.headers.get('opc-request-id')}")

        return (202, json.dumps({"status": "enqueued", "channel": channel, "result": result}))

    except Exception as e:
        print(f"Error en ejecución: {e}")
        return (500, json.dumps({"error": f"Internal error: {str(e)}"}))
