import io
import json
import os
import requests
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# --- Variables de entorno ---
OSB_AUTH = os.getenv("OSB_AUTH")

# --- Mapa de canales a endpoints ---
CHANNEL_ENDPOINTS = {
    "CANAL_NOTIFICACIONES_POMELO": os.getenv("OSB_BASE_URL_POMELO"),
    "CANAL_NOTIFICACIONES_ACTIVIDADES": os.getenv("OSB_BASE_URL_ACTIVIDADES"),
    "CANAL_EVENTOS_TARJETA": os.getenv("OSB_BASE_URL_TARJETA")
}

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        events = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        logger.error(f"Invalid JSON: {e}")
        return (400, json.dumps({"error": f"Invalid JSON: {e}"}))

    results = []
    events = events if isinstance(events, list) else [events]

    for ev in events:
        payload = ev.get("payload", {})
        channel = ev.get("Channel") or ev.get("canal")  # según cómo venga

        if not channel or channel not in CHANNEL_ENDPOINTS:
            logger.warning(f"Channel inválido o no soportado: {channel}")
            results.append({
                "status": "error",
                "message": f"Channel inválido o no soportado: {channel}"
            })
            continue

        endpoint = CHANNEL_ENDPOINTS[channel]
        headers = {
            "Content-Type": "application/json",
            "Authorization": OSB_AUTH
        }

        try:
            logger.info(f"Enviando payload al endpoint {endpoint} para channel {channel}")
            r = requests.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=10,
                verify=True
            )
            status = r.status_code
            logger.info(f"POST enviado a {endpoint}, status={status}")
        except Exception as e:
            status = f"error: {str(e)}"
            logger.error(f"Error enviando a webhook: {status}")

        results.append({
            "channel": channel,
            "status": status
        })

    summary = {"processed": results}
    logger.info(f"Resumen final: {summary}")
    return (200, json.dumps(summary, ensure_ascii=False),
            {"Content-Type": "application/json"})
