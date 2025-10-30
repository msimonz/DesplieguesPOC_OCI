import io
import json
import logging
import hmac
import hashlib
import base64
import os
import time
import requests
from fdk import response

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
API_SECRET = os.getenv("API_SECRET", "")
# === CONFIGURACIÓN DE LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# === FUNCIONES AUXILIARES ===

def get_api_secret(api_secret_key):
    """Decodifica el secreto en base64."""
    return base64.b64decode(api_secret_key)

def check_signature(api_secret, endpoint, timestamp, body, received_signature):
    """Valida la firma HMAC-SHA256 recibida contra la calculada localmente."""
    try:
        logger.info("Inicia validación de firma")

        # Verificar formato del encabezado
        if not received_signature.startswith("hmac-sha256 "):
            logger.warning("Formato de firma inválido: falta el prefijo 'hmac-sha256'")
            return False

        # Quitar prefijo
        received_signature = received_signature[len("hmac-sha256 "):]

        # Reconstruir el mensaje firmado
        secret = get_api_secret(api_secret)
        message = (timestamp + endpoint + (body or "")).encode("utf-8")

        # Calcular HMAC
        hash_obj = hmac.new(secret, message, hashlib.sha256)
        calculated_signature = base64.b64encode(hash_obj.digest()).decode()

        # Logs para diagnóstico
        logger.info(f"Body recibida  : {body}")
        logger.info(f"Firma recibida  : {received_signature}")
        logger.info(f"Firma calculada : {calculated_signature}")
        logger.info(f"Api-Secret valor: {api_secret}")
        # Comparación segura
        return hmac.compare_digest(base64.b64decode(received_signature), hash_obj.digest())

    except Exception as e:
        logger.error(f"Error al validar la firma: {e}")
        return False

# === MANEJADOR PRINCIPAL ===

def handler(ctx, data: io.BytesIO = None):
    try:

        # Leer cuerpo y cabeceras
        input_body = data.getvalue().decode("utf-8") if data else ""
        headers = ctx.Headers()

        endpoint = headers.get("x-endpoint", "")
        timestamp = headers.get("x-timestamp", "")
        signature = headers.get("x-signature", "")
        apikey = headers.get("x-api-key", "")

        logger.info(f"Headers recibidos: {headers}")
        logger.info(f"Cuerpo recibido  : {input_body}")

        # Validar parámetros obligatorios
        if not all([endpoint, timestamp, signature, apikey, input_body]):
            logger.warning("Petición con parámetros faltantes")
            return response.Response(
                ctx,
                response_data=json.dumps({"errorCode": 400, "errorMessage": "Mensaje no válido"}),
                headers={"Content-Type": "application/json"},
                status_code=400
            )

        # Validar firma
        if not check_signature(API_SECRET, endpoint, timestamp, input_body, signature):
            logger.warning("Firma inválida. Se rechaza la petición.")
            return response.Response(
                ctx,
                response_data=json.dumps({"errorCode": 401, "errorMessage": "Firma no válida"}),
                headers={"Content-Type": "application/json"},
                status_code=401
            )

        # Si la firma es válida → reenviar al webhook
        logger.info("Firma válida ✅. Enviando al webhook...")

        r = requests.post(
            WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            data=input_body,
            timeout=30
        )

        logger.info(f"Respuesta del webhook: {r.status_code} - {r.text}")

        return response.Response(
            ctx,
            response_data=json.dumps({
                "status": "Mensaje enviado correctamente",
                "forward_status": r.status_code,
            }),
            headers={"Content-Type": "application/json"},
            status_code=200
        )

    except Exception as e:
        logger.exception("Error general en la función")
        return response.Response(
            ctx,
            response_data=json.dumps({"errorCode": 500, "errorMessage": str(e)}),
            headers={"Content-Type": "application/json"},
            status_code=500
        )
