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

# === VARIABLES DE ENTORNO ===
OSB_BASE_URL = os.getenv("OSB_BASE_URL")
OSB_AUTH = os.getenv("OSB_AUTH")
API_SECRET = os.getenv("API_SECRET", "")

# === CONFIGURACIÓN DE LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# === FUNCIONES AUXILIARES ===
def get_api_secret(api_secret_key):
    """Decodifica el secreto en base64."""
    return base64.b64decode(api_secret_key)

def sign_response(api_secret, body, headers, endpoint):
    """Genera la firma HMAC-SHA256 para la respuesta."""
    timestamp = str(int(time.time()))
    secret = get_api_secret(api_secret)
    message = (timestamp + endpoint + (body or "")).encode('utf-8')
    hash_obj = hmac.new(secret, message, hashlib.sha256)
    calculated_signature = base64.b64encode(hash_obj.digest()).decode()

    headers["x-endpoint"] = endpoint
    headers["x-timestamp"] = timestamp
    headers["x-signature"] = f"hmac-sha256 {calculated_signature}"

def check_signature(api_secret, endpoint, timestamp, body, received_signature):
    """Valida la firma HMAC-SHA256 recibida contra la calculada localmente."""
    try:
        logger.info("Inicia validación de firma")

        if not received_signature.startswith("hmac-sha256 "):
            logger.warning("Formato de firma inválido: falta el prefijo 'hmac-sha256'")
            return False

        received_signature = received_signature[len("hmac-sha256 "):]
        secret = get_api_secret(api_secret)
        message = (timestamp + endpoint + (body or "")).encode("utf-8")

        hash_obj = hmac.new(secret, message, hashlib.sha256)
        calculated_signature = base64.b64encode(hash_obj.digest()).decode()

        logger.info(f"Firma recibida  : {received_signature}")
        logger.info(f"Firma calculada : {calculated_signature}")

        return hmac.compare_digest(base64.b64decode(received_signature), hash_obj.digest())

    except Exception as e:
        logger.error(f"Error al validar la firma: {e}")
        return False

# === MANEJADOR PRINCIPAL ===
def handler(ctx, data: io.BytesIO = None):
    try:
        # Leer cuerpo y cabeceras
        input_body = data.getvalue().decode("utf-8") if data else ""
        in_headers = ctx.Headers()

        endpoint = in_headers.get("x-endpoint", "")
        timestamp = in_headers.get("x-timestamp", "")
        signature = in_headers.get("x-signature", "")
        apikey = in_headers.get("x-api-key", "")

        logger.info(f"Headers recibidos: {in_headers}")
        logger.info(f"Cuerpo recibido  : {input_body}")

        # Validar parámetros obligatorios
        if not all([endpoint, timestamp, signature, apikey, input_body]):
            logger.warning("Petición con parámetros faltantes")
            response_headers = {"Content-Type": "application/json"}
            body_out = json.dumps({"errorCode": 400, "errorMessage": "Parámetros incompletos"})
            sign_response(API_SECRET, body_out, response_headers, endpoint)
            return response.Response(ctx, response_data=body_out, status_code=400, headers=response_headers)

        # Validar firma HMAC
        if not check_signature(API_SECRET, endpoint, timestamp, input_body, signature):
            response_headers = {"Content-Type": "application/json"}
            body_out = json.dumps({"errorCode": 400, "errorMessage": "Firma no válida"})
            sign_response(API_SECRET, body_out, response_headers, endpoint)
            return response.Response(ctx, response_data=body_out, status_code=400, headers=response_headers)

        # Enviar al OSB
        out_headers = {
            "Content-Type": "application/json",
            "Authorization": OSB_AUTH,
        }
        r = requests.post(
            OSB_BASE_URL,
            headers=out_headers,
            data=input_body,
            timeout=30,
            verify=True
        )

        logger.info(f"Respuesta OSB: {r.status_code} - {r.text}")

        response_headers = {"Content-Type": "application/json"}

        if r.status_code == 204:
            sign_response(API_SECRET, "", response_headers, endpoint)
            return response.Response(ctx, status_code=204, headers=response_headers)
        else:
            body_out = json.dumps({
                "status": "Error en la petición POST",
                "osb_status": r.status_code
            })
            sign_response(API_SECRET, body_out, response_headers, endpoint)
            return response.Response(ctx, response_data=body_out, status_code=r.status_code, headers=response_headers)

    except Exception as e:
        logger.exception("Error general en la función")
        return response.Response(
            ctx,
            response_data=json.dumps({"errorCode": 500, "errorMessage": str(e)}),
            headers={"Content-Type": "application/json"},
            status_code=500
        )
