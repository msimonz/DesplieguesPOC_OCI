import io, json, os
import oci
import logging
from fdk import response

QUEUE_OCID = os.getenv("QUEUE_OCID")

# === CONFIGURACIÓN DE LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def _get_header(headers: dict, name: str):
    if not headers:
        return None
    lower = {k.lower(): v for k, v in headers.items()}
    return lower.get(name.lower())

def _extract_path_params(ctx):
    try:
        req_url = ctx.RequestURL()  # URL completa de la invocación
        parts = req_url.split("/")

        if "abort" in parts:
            params = parts[-2] if len(parts) > 1 else None
        elif "commit" in parts:
            params = parts[-2] if len(parts) > 1 else None
        elif "intents" in parts:
            params = parts[-1] if len(parts) > 1 else None
        else:
            return "prepared"
        return params
    except Exception as e:
        logger.error(f"Error extrayendo parámetros de URL: {e}")
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
        logger.info(f"[fn_producer_queue_minka_debit] Headers recibidos: {headers}")

        # Extraer parámetros de la URL
        path_params = _extract_path_params(ctx)
        logger.info(f"[fn_producer_queue_minka_debit] Parámetros extraídos de la URL: {path_params}")

        logger.info(f"[fn_producer_queue_minka_debit] body: {body}")

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

        if channel == "Completed":
            status = (body.get("data", {}).get("intent", {}).get("meta", {}).get("status"))
            if status != "completed":
                return response.Response(
                    ctx,
                    response_data=json.dumps({
                        "code": 200,
                        "message": f"Mensaje recibido exitosamente estado: {status}"
                    }),
                    status_code=200,
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

        # Cargar credenciales OCI
        file_config = oci.config.from_file("config.oci")

        # Obtener endpoint de la Queue
        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint

        # Crear cliente de mensajes
        queue_client = oci.queue.QueueClient(config=file_config)
        queue_client.base_client.endpoint = messages_endpoint
        enriched_body = {}
        if path_params == "prepared":
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
        # Construir el mensaje
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
        # Enviar mensaje a la Queue
        resp = queue_client.put_messages(
            queue_id=QUEUE_OCID,
            put_messages_details=put_details
        )
    
        result = oci.util.to_dict(resp.data)
        logger.info(f"[fn_producer_queue_minka_debit] put_messages in channel={channel}, result={result}")

        if channel == "Completed":
            statusHttp = "200"
        else:
            statusHttp = "202"

        return response.Response(
            ctx,
            response_data=json.dumps({
                "code": statusHttp,
                "message": "Mensaje encolado correctamente",
            }),
            status_code=statusHttp,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        logger.error(f"Producer Error en ejecución: {e}")
        return response.Response(
            ctx,
            response_data=json.dumps({
                "code": 500,
                "message": f"Error al enviar mensaje. Detalle: {str(e)}"
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )