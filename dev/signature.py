import base64
import hashlib
import hmac

timestamp = "1774897592"
endpoint = "/api/exc/rest/b2b/pomelo/V1.0/cards/events"
api_secret_b64 = "hByKI5U+zzpMibm7MiEnjEsnBHC4ntATnEhjzKRw2fw="

body = """{
    "event_id":"card-block",
    "id":"crd-3BPOqf2EH14E70Jdbs6WuYc6mbH",
    "updated_at":"2026-03-30T19:04:43.042289Z",
    "user_id":"usr-3AuR38ZZNL7QcU2D7ybyPn2G6YS",
    "event":"BLOCK","card_type":"VIRTUAL",
    "idempotency_key":"82d10aa1-0df7-488f-8a8c-4fcdca0fe8d8"
}"""

secret = base64.b64decode(api_secret_b64)
string_to_sign = timestamp + endpoint + body
digest = hmac.new(secret, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
signature_b64 = base64.b64encode(digest).decode()

print("endpoint: ", repr(endpoint))
print("timestamp: ", repr(timestamp))
print("body: ", repr(body))
print("body sha256:", hashlib.sha256(body.encode("utf-8")).hexdigest())
print("string_to_sign: ", string_to_sign)
#print("body len:", len(body))
print("string_to_sign sha256:", hashlib.sha256(string_to_sign.encode("utf-8")).hexdigest())
#print("secret fingerprint:", hashlib.sha256(secret).hexdigest()[:12])
print("FIRMA CORRECTA:", f"hmac-sha256 {signature_b64}")