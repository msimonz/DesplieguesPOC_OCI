import io
import json
import os
import requests
import logging
import oci
import time

# === CONFIGURACIÓN GENERAL ===
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
QUEUE_OCID = os.getenv("QUEUE_OCID")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
VISIBILITY_DELAY = int(os.getenv("VISIBILITY_DELAY", "120"))  # segundos

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
    Reenvía el mensaje a la Queue para reintento usando la API REST firmada.
    Aplica un visibilityInSeconds real antes de que el mensaje vuelva a ser visible.
    """
    try:
        # 1️⃣ Cargar configuración de OCI y crear signer
        file_config = oci.config.from_file("config.oci")
        signer = oci.signer.Signer(
            tenancy=file_config["tenancy"],
            user=file_config["user"],
            fingerprint=file_config["fingerprint"],
            private_key_file_location=file_config["key_file"]
        )

        # 2️⃣ Obtener endpoint de la Queue
        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint

        # 3️⃣ Construir mensaje con visibilidad retrasada
        enriched_body = {"payload": payload, "channel": channel}
        message_data = {
            "messages": [
                {
                    "content": json.dumps(enriched_body),
                    "metadata": {"channelId": str(channel)},
                    "visibilityInSeconds": VISIBILITY_DELAY  # 👈 parámetro correcto
                }
            ]
        }

        url = f"{messages_endpoint}/20210201/queues/{QUEUE_OCID}/messages"
        headers = {"Content-Type": "application/json"}

        # 4️⃣ Enviar mensaje firmado a la cola
        response = requests.post(url, data=json.dumps(message_data),
                                 headers=headers, auth=signer)

        if response.status_code == 200:
            logger.info(
                f"Mensaje reenviado a Queue (reintento #{payload.get('retry_count', 0)}, "
                f"visibility={VISIBILITY_DELAY}s)"
            )
            return True
        else:
            logger.error(
                f"Error reenviando mensaje (HTTP {response.status_code}): {response.text}"
            )
            return False

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

            if status >= 400:
                raise Exception(f"HTTP {status}")

        except Exception as e:
            retry_count += 1
            payload["retry_count"] = retry_count
            logger.error(f"Error enviando a webhook: {str(e)}. Reintento #{retry_count}")

            if retry_count <= MAX_RETRIES:
                # Esperar un poco antes de reencolar (opcional)
                #logger.info(f"Esperando {VISIBILITY_DELAY}s antes de reencolar mensaje...")
                #time.sleep(VISIBILITY_DELAY)
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
