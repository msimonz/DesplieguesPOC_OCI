import base64
import hmac
import hashlib

def get_api_secret(api_secret_key):
    """Decodifica el secreto en base64."""
    return base64.b64decode(api_secret_key)

def check_signature(api_secret, endpoint, timestamp, body, received_signature):
    """Valida la firma HMAC-SHA256 recibida contra la calculada localmente."""
    try:
        # Verificar formato del encabezado
        if not received_signature.startswith("hmac-sha256 "):
            return False

        # Quitar prefijo
        received_signature = received_signature[len("hmac-sha256 "):]

        # Reconstruir el mensaje firmado
        secret = get_api_secret(api_secret)
        message = (timestamp + endpoint + (body or "")).encode("utf-8")

        # Calcular HMAC
        hash_obj = hmac.new(secret, message, hashlib.sha256)
        calculated_signature = base64.b64encode(hash_obj.digest()).decode()

        # Comparación segura
        return hmac.compare_digest(base64.b64decode(received_signature), hash_obj.digest()), calculated_signature

    except Exception as e:
        return False

timestamp = "1730505600"
endpoint = "/pomelo/eventosTarjeta/V1.0"
body = '{\"event_id\": \"event_id2\", \"id\": \"crd-2xotdFOZi4krMztOWReg83HJDat\",\"updated_at\": \"2025-06-10T14:15:31.186Z\",\"user_id\":\"usr-2xotd8DAN3T21wKLkZoltJUkCyw\",\"event\": \"BLOCK\",\"card_type\": \"VIRTUAL\",\"related_card_id\":\"crd-2xotdFOZi4krMztOWReg83HJDau\",\"idempotency_key\": \"idempotency_key2\"}'
api_secret = "hByKl5U+zzpMibm7MiEnjEsnBHC4ntATnEhjzKRw2fw= "
message = (timestamp + endpoint + body).encode("utf-8")
secret = get_api_secret(api_secret)
print("Secret:", secret)
signature = base64.b64encode(hmac.new(secret, message, hashlib.sha256).digest())
print("Calculated Signature:", signature)

compare, calculated_signature = check_signature(api_secret, endpoint, timestamp, body, "hmac-sha256 " + signature.decode())
print("Signature Match:", compare)
print("Calculated Signature from Function:", calculated_signature)