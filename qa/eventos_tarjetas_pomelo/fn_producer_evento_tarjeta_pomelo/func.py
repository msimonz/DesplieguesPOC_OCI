import io
import json
import os
import logging
import oci
from fdk import response

# === CONFIGURACIÓN DE LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# === VARIABLES DE ENTORNO ===
QUEUE_OCID = os.getenv("QUEUE_OCID")

# === CANALES PERMITIDOS ===
VALID_CHANNELS = {
    "CANAL_NOTIFICACIONES_POMELO",
    "CANAL_NOTIFICACIONES_ACTIVIDADES",
    "CANAL_EVENTOS_TARJETA"
}


def handler(ctx, data: io.BytesIO = None):
    logger.info("=== [Inicio de ejecución de la Function] ===")

    # --- Leer y parsear JSON ---
    try:
        raw_body = data.getvalue() if data else b"{}"
        body_str = raw_body.decode("utf-8")
        logger.info(f"Payload recibido (raw): {body_str}")

        body = json.loads(body_str)
        logger.info("JSON parseado correctamente.")
    except Exception as e:
        logger.error(f"Error parseando JSON: {e}", exc_info=True)
        return response.Response(
            ctx,
            response_data=json.dumps({"code": 400, "message": f"JSON inválido: {e}"}),
            status_code=400,
            headers={"Content-Type": "application/json"}
        )

    # --- Validar Headers ---
    try:
        headers = ctx.Headers() if hasattr(ctx, "Headers") else {}
        logger.info("=== Headers recibidos ===")
        for k, v in headers.items():
            logger.info(f"{k}: {v}")

        # Obtener el canal del header
        channel_queue = headers.get("channel_queue", "").strip()
        if not channel_queue:
            logger.warning("Header 'channel_queue' no encontrado o vacío.")
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "code": 400,
                    "message": "Header 'channel_queue' es requerido"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        # Validar canal permitido
        if channel_queue not in VALID_CHANNELS:
            logger.warning(f"Canal no permitido: {channel_queue}")
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "code": 400,
                    "message": f"Canal no válido. Debe ser uno de: {', '.join(VALID_CHANNELS)}"
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )

        if not QUEUE_OCID:
            logger.error("Variable de entorno QUEUE_OCID no configurada.")
            return response.Response(
                ctx,
                response_data=json.dumps({
                    "code": 500,
                    "message": "QUEUE_OCID no configurado en variables de entorno"
                }),
                status_code=500,
                headers={"Content-Type": "application/json"}
            )

        # --- Configuración OCI ---
        logger.info("Cargando configuración OCI desde 'config.oci'...")
        file_config = oci.config.from_file("config.oci")
        logger.info(f"Configuración cargada para el tenant: {file_config.get('tenancy')}")

        # --- Obtener endpoint de la Queue ---
        logger.info(f"Obteniendo endpoint de la Queue con OCID: {QUEUE_OCID}")
        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint
        logger.info(f"Endpoint de mensajes: {messages_endpoint}")

        # --- Crear cliente de Queue ---
        queue_client = oci.queue.QueueClient(config=file_config)
        queue_client.base_client.endpoint = messages_endpoint
        logger.info("Cliente de Queue configurado correctamente.")

        # --- Preparar mensaje con canal ---
        enriched_body = {
            "Channel": channel_queue,
            "payload": body
        }
        logger.info(f"Mensaje a encolar: {json.dumps(enriched_body)}")

        put_details = oci.queue.models.PutMessagesDetails(
            messages=[
                oci.queue.models.PutMessagesDetailsEntry(
                    content=json.dumps(enriched_body),
                    metadata={
                        "channelId": str(channel_queue)
                    }
                )
            ]
        )

        # --- Enviar mensaje ---
        logger.info(f"Enviando mensaje al canal '{channel_queue}' de la Queue...")
        resp = queue_client.put_messages(
            queue_id=QUEUE_OCID,
            put_messages_details=put_details
        )

        logger.info("Mensaje encolado correctamente.")
        for msg in resp.data.messages:
            logger.info(
                f"Mensaje ID={msg.id}, ErrorCode={getattr(msg, 'error_code', None)}, "
                f"ErrorMessage={getattr(msg, 'error_message', None)}"
            )


        return response.Response(
            ctx,
            response_data=json.dumps({
                "code": 202,
                "message": f"Mensaje encolado correctamente en canal '{channel_queue}'",
            }),
            status_code=202,
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        logger.exception(f"Error durante la ejecución: {e}")
        return response.Response(
            ctx,
            response_data=json.dumps({
                "code": 500,
                "message": f"Error al enviar mensaje. Detalle: {str(e)}"
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )
