import io
import json
import os
import requests
import logging
import oci
import time

# === CONFIGURACIÓN GENERAL ===
OSB_BASE_URL = os.getenv("OSB_BASE_URL")  
OSB_AUTH = os.getenv("OSB_AUTH")          
QUEUE_OCID = os.getenv("QUEUE_OCID")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
VISIBILITY_DELAY = int(os.getenv("VISIBILITY_DELAY", "120")) 

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()


def _get_header(headers: dict, name: str):
    if not headers:
        return None
    lower = {k.lower(): v for k, v in headers.items()}
    return lower.get(name.lower())


def _send_back_to_queue(payload, channel, path_params):
    """Reenvía el mensaje a la Queue OCI con retraso de visibilidad."""
    try:
        file_config = oci.config.from_file("config.oci")
        signer = oci.signer.Signer(
            tenancy=file_config["tenancy"],
            user=file_config["user"],
            fingerprint=file_config["fingerprint"],
            private_key_file_location=file_config["key_file"]
        )

        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint

        enriched_body = {"payload": payload, "pathParams": path_params, "channel": channel}
        message_data = {
            "messages": [
                {
                    "content": json.dumps(enriched_body),
                    "metadata": {"channelId": str(channel)},
                    "deliveryDelayInSeconds": VISIBILITY_DELAY
                }
            ]
        }

        url = f"{messages_endpoint}/20210201/queues/{QUEUE_OCID}/messages"
        headers = {"Content-Type": "application/json"}

        response = requests.post(url, data=json.dumps(message_data),
                                 headers=headers, auth=signer)

        if response.status_code == 200:
            logger.info(f"[fn_consumer_queue_minka_debit] Mensaje reenviado a Queue (retry={payload.get('retry_count', 0)}, delay={VISIBILITY_DELAY}s)")
            return True
        else:
            logger.error(f"[fn_consumer_queue_minka_debit] Error reenviando mensaje (HTTP {response.status_code}): {response.text}")
            return False

    except Exception as e:
        logger.error(f"[fn_consumer_queue_minka_debit] Error reenviando a la Queue: {e}")
        return False


def _build_osb_endpoint(channel, path_params):
    if not OSB_BASE_URL:
        raise ValueError("OSB_BASE_URL no configurado en variables de entorno.")
    if channel == "Prepared":
        return f"{OSB_BASE_URL}/crear"
    elif channel == "Aborted":
        return f"{OSB_BASE_URL}/{str(path_params)}/abortar"
    elif channel == "Committed":
        return f"{OSB_BASE_URL}/{str(path_params)}/confirmar"
    elif channel == "Completed":
        return f"{OSB_BASE_URL}/{str(path_params)}/completar"
    # fallback genérico
    return OSB_BASE_URL


def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        event = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        logger.error(f"[fn_consumer_queue_minka_debit] Invalid JSON: {e}")
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    results = []
    events = event if isinstance(event, list) else [event]

    for ev in events:
        payload = ev.get("payload", {})
        channel = ev.get("channel", "unknown")
        # Si el pathParams viene dentro de payload (estructura anidada)
        path_params = ev.get("pathParams") or ev.get("payload", {}).get("pathParams", "")
        retry_count = payload.get("retry_count", 0)
        logger.info(f"[fn_consumer_queue_minka_debit] EVENTO: {json.dumps(ev)}")
        logger.info(f"[fn_consumer_queue_minka_debit] Channel: {channel}")
        logger.info(f"[fn_consumer_queue_minka_debit] PathParams: {json.dumps(path_params)}")
        logger.info(f"[fn_consumer_queue_minka_debit] Payload: {json.dumps(payload)[:500]}")

        try:
            osb_endpoint = _build_osb_endpoint(channel, path_params)
            logger.info(f"[fn_consumer_queue_minka_debit] Endpoint OSB seleccionado: {osb_endpoint}")

            headers = {
                "Content-Type": "application/json",
                "Authorization": OSB_AUTH,
                "Channel": channel,
                "Retry-Count": str(retry_count)
            }
            status = None
            if channel == "Completed":
                response = requests.put(osb_endpoint, json=payload, headers=headers, timeout=15, verify=True)
                status = response.status_code
            else:
                response = requests.post(osb_endpoint, json=payload, headers=headers, timeout=15, verify=True)
                status = response.status_code

            logger.info(f"[fn_consumer_queue_minka_debit] Solicitud enviada a OSB: {osb_endpoint}, status={status}")
            logger.info(f"[fn_consumer_queue_minka_debit] Respuesta OSB: {response.text[:500]}")

            if status >= 400:
                raise Exception(f"HTTP {status}")

        except Exception as e:
            retry_count += 1
            payload["retry_count"] = retry_count
            logger.error(f"[fn_consumer_queue_minka_debit] Error enviando a OSB: {str(e)}. Reintento #{retry_count}")

            if retry_count <= MAX_RETRIES:
                ok = _send_back_to_queue(payload, channel, path_params)
                status = f"requeued (retry #{retry_count})" if ok else f"failed to requeue (retry #{retry_count})"
            else:
                status = f"max retries exceeded ({MAX_RETRIES})"

        results.append({
            "channel": channel,
            "status": status,
            "retry_count": retry_count
        })

    summary = {"processed": results}
    logger.info(f"[fn_consumer_queue_minka_debit] Resumen final: {summary}")

    return (200, json.dumps(summary, ensure_ascii=False),
            {"Content-Type": "application/json"})