import io, json, os
import oci
from fdk import response

QUEUE_OCID = os.getenv("QUEUE_OCID")

def _get_header(headers: dict, name: str):
    if not headers:
        return None
    lower = {k.lower(): v for k, v in headers.items()}
    return lower.get(name.lower())

def _extract_path_params(ctx):
    try:
        req_url = ctx.RequestURL()  # URL completa de la invocación
        # Ejemplo: https://dev-api.alianza.com.co/api/transacciones/rest/b2b/fiducia/minka/V1.0/abc123/debito-abortar
        parts = req_url.split("/")

        # buscamos si contiene 'debito-preparar', 'debito-abortar', 'debito-confirmar' o 'debito-completar'
        if "debito-preparar" in parts:
            return "preparar"
        elif "debito-abortar" in parts:
            params = parts[-2] if len(parts) > 1 else None
        elif "debito-confirmar" in parts:
            params = parts[-2] if len(parts) > 1 else None
        elif "debito-completar" in parts:
            params = parts[-2] if len(parts) > 1 else None
        return params
    except Exception as e:
        print(f"Error extrayendo parámetros de URL: {e}")
        return {}

def handler(ctx, data: io.BytesIO = None):
    try:
        raw_body = data.getvalue() if data else b"{}"
        body = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        return response.Response(
            ctx,
            response_data=json.dumps({"code": 400, "message": f"JSON inválido: {e}"}),
            status_code=400,
            headers={"Content-Type": "application/json"}
        )

    try:
        headers = ctx.Headers() if hasattr(ctx, "Headers") else {}
        print("=== Headers recibidos ===")
        print(headers)

        # Extraer parámetros de la URL
        path_params = _extract_path_params(ctx)
        print("=== Parámetros extraídos de la URL ===")
        print(path_params)

        # Obtener canal desde header o body
        channel = _get_header(headers, "x-queue-channel") or body.get("channel")
        if not channel:
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "code": 400,
                    "message": "Falta el parámetro 'channel' (header x-queue-channel o body.channel)"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        if not QUEUE_OCID:
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "code": 500,
                    "message": "QUEUE_OCID no configurado en variables de entorno"
                }),
                status_code=500,
                headers={"Content-Type": "application/json"}
            )

        # 1️⃣ Cargar credenciales OCI
        file_config = oci.config.from_file("config.oci")

        # 2️⃣ Obtener endpoint de la Queue
        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint

        # 3️⃣ Crear cliente de mensajes
        queue_client = oci.queue.QueueClient(config=file_config)
        queue_client.base_client.endpoint = messages_endpoint
        enriched_body = {}
        if path_params == "preparar":
            enriched_body = {
                "payload": body,
                "channel": channel,
            }
        else:
            enriched_body = {
                "payload": body,
                "pathParams": path_params,
                "channel": channel
            }
        # 4️⃣ Construir el mensaje
        put_details = oci.queue.models.PutMessagesDetails(
            messages=[
                oci.queue.models.PutMessagesDetailsEntry(
                    content=json.dumps(enriched_body),
                    metadata={
                        "channelId": str(channel),
                        "pathParams": json.dumps(path_params)
                    }
                )
            ]
        )
        # 5️⃣ Enviar mensaje a la Queue
        resp = queue_client.put_messages(
            queue_id=QUEUE_OCID,
            put_messages_details=put_details
        )
    
        result = oci.util.to_dict(resp.data)
        print(f"put_messages opc-request-id={resp.headers.get('opc-request-id')}")

        return response.Response(
            ctx,
            response_data=json.dumps({
                "code": 202,
                "message": "Mensaje encolado correctamente",
            }),
            status_code=202,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        print(f"Error en ejecución: {e}")
        return response.Response(
            ctx,
            response_data=json.dumps({
                "code": 500,
                "message": f"Error al enviar mensaje. Detalle: {str(e)}"
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )
