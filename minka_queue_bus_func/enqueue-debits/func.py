import io, json, os
import oci

QUEUE_OCID = os.getenv("QUEUE_OCID")  # OCID de la Queue

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        body = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    try:
        # Headers reales desde el request
        headers = ctx.Headers() if hasattr(ctx, "Headers") else {}
        print("=== Headers recibidos ===")
        print(headers)

        channel = headers.get("x-queue-channel")
        if not channel:
            return (400, json.dumps({"error": "Missing channel header in request"}))

        if not QUEUE_OCID:
            return (500, json.dumps({
                "status": "failed",
                "reason": "QUEUE_OCID not configured",
                "Channel": channel
            }))

        signer = oci.auth.signers.get_resource_principals_signer()
        queue_client = oci.queue.QueueClient(config={}, signer=signer)

        print(channel)
        enriched_body = {
            "payload": body,
            "channel": channel
        }

        put_details = oci.queue.models.PutMessagesDetails(
            messages=[
                oci.queue.models.PutMessagesDetailsEntry(
                    content=json.dumps(enriched_body),
                    metadata={"channelId": channel}   # 👈 aquí va el canal
                )
            ]
        )


        resp = queue_client.put_messages(
            queue_id=QUEUE_OCID,
            put_messages_details=put_details
        )

        return (202, json.dumps({
            "status": "enqueued",
            "channel": channel,
            "result": oci.util.to_dict(resp.data)
        }))

    except Exception as e:
        print(f"Error en ejecución: {e}")
        return (500, json.dumps({"error": f"Internal error: {str(e)}"}))
