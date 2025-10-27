import io
import json
import os
import requests
import logging
import oci

# === CONFIGURACIÓN GENERAL ===
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
QUEUE_OCID = os.getenv("QUEUE_OCID")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()


def _get_header(headers: dict, name: str):
    """Obtiene un header sin importar mayúsculas/minúsculas"""
    if not headers:
        return None
    lower = {k.lower(): v for k, v in headers.items()}
    return lower.get(name.lower())


def _send_back_to_queue(payload, channel):
    """
    Reenvía el mensaje a la Queue para reintento.
    Usa config.oci para autenticación (no Resource Principals).
    """
    try:
        # 1️⃣ Cargar configuración local de OCI (config.oci)
        file_config = oci.config.from_file("config.oci")

        # 2️⃣ Obtener endpoint de mensajes de la Queue
        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint

        # 3️⃣ Crear QueueClient apuntando al endpoint
        queue_client = oci.queue.QueueClient(config=file_config)
        queue_client.base_client.endpoint = messages_endpoint

        # 4️⃣ Construir mensaje con metadata
        enriched_body = {"payload": payload, "channel": channel}
        put_details = oci.queue.models.PutMessagesDetails(
            messages=[
                oci.queue.models.PutMessagesDetailsEntry(
                    content=json.dumps(enriched_body),
                    metadata={"channelId": str(channel)}
                )
            ]
        )

        # 5️⃣ Enviar mensaje a la Queue
        resp = queue_client.put_messages(queue_id=QUEUE_OCID, put_messages_details=put_details)
        logger.info(f"Mensaje reenviado a Queue (reintento #{payload.get('retry_count', 0)}). "
                    f"opc-request-id={resp.headers.get('opc-request-id')}")
        return True

    except Exception as e:
        logger.error(f"Error reenviando a la Queue: {e}")
        return False


def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        event = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        logger.error(f"Invalid JSON: {e}")
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    results = []
    events = event if isinstance(event, list) else [event]

    for ev in events:
        payload = ev.get("payload", {})
        channel = ev.get("channel", "unknown")
        retry_count = payload.get("retry_count", 0)

        logger.info("=== Evento recibido ===")
        logger.info(f"Channel: {channel}")
        logger.info(f"Payload completo: {json.dumps(payload)[:500]}")

        try:
            headers = {"Channel": channel, "Retry-Count": str(retry_count)}

            if channel == "Completed":
                r = requests.put(WEBHOOK_URL, json=payload, headers=headers, timeout=10)
            else:
                r = requests.post(WEBHOOK_URL, json=payload, headers=headers, timeout=10)

            status = r.status_code
            logger.info(f"Webhook enviado a {WEBHOOK_URL}, status={status}")

            # Si falla (>=400), generar reintento
            if status >= 400:
                raise Exception(f"HTTP {status}")

        except Exception as e:
            retry_count += 1
            payload["retry_count"] = retry_count
            logger.error(f"Error enviando a webhook: {str(e)}. Reintento #{retry_count}")

            if retry_count <= MAX_RETRIES:
                ok = _send_back_to_queue(payload, channel)
                if ok:
                    status = f"requeued (retry #{retry_count})"
                else:
                    status = f"failed to requeue (retry #{retry_count})"
            else:
                status = f"max retries exceeded ({MAX_RETRIES})"

        results.append({
            "channel": channel,
            "status": status,
            "retry_count": retry_count
        })

    summary = {"processed": results}
    logger.info(f"Resumen final: {summary}")

    return (200, json.dumps(summary, ensure_ascii=False),
            {"Content-Type": "application/json"})
