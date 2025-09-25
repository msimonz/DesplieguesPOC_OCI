import io
import json
import os
import requests
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

WEBHOOK_URL = os.getenv("WEBHOOK_URL")

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

        logger.info("=== Evento recibido ===")
        logger.info(f"Channel: {channel}")
        logger.info(f"Payload completo: {json.dumps(payload)[:500]}")

        try:
            if channel == "Completed":
                r = requests.put(
                    WEBHOOK_URL,
                    json=payload,                     # 👈 Aquí mandamos TODO el objeto
                    headers={"Channel": channel},     # 👈 Header extra
                    timeout=10
                )
                status = r.status_code
                logger.info(f"POST enviado a {WEBHOOK_URL}, status={status}")
            else:
                r = requests.post(
                    WEBHOOK_URL,
                    json=payload,                     # 👈 Aquí mandamos TODO el objeto
                    headers={"Channel": channel},     # 👈 Header extra
                    timeout=10
                )
                status = r.status_code
                logger.info(f"POST enviado a {WEBHOOK_URL}, status={status}")
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
