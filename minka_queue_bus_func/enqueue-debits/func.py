import io, json, os
import oci

# Variables de entorno (se configuran en func.yaml o en la consola)
QUEUE_OCID = os.getenv("QUEUE_OCID")  # OCID de la Queue
CHANNEL_HEADER = os.getenv("CHANNEL_HEADER", "x-queue-channel")

def handler(ctx, data: bytes = None):
    try:
        body = {} if not data else json.loads(data.decode("utf-8"))
    except Exception as e:
        return (400, f"Invalid JSON: {e}")

    # Headers desde API Gateway (Fn las pasa en ctx.Config si los defines como "config")
    headers = ctx.Config() or {}
    channel = headers.get(CHANNEL_HEADER)

    if not channel:
        return (400, "Missing channel header")

    if not QUEUE_OCID:
        return (500, "QUEUE_OCID not configured")

    signer = oci.auth.signers.get_resource_principals_signer()
    queue_client = oci.queue.QueueClient(config={}, signer=signer)

    put_details = oci.queue.models.PutMessagesDetails(
        messages=[
            oci.queue.models.PutMessagesDetailsEntry(
                content=json.dumps(body),
                channel=channel
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
