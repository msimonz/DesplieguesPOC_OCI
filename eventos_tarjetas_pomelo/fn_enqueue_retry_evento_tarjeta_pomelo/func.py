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

QUEUE_OCID = os.getenv("QUEUE_OCID")

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

    # --- Validar y ejecutar envío a Queue ---
    try:
        headers = ctx.Headers() if hasattr(ctx, "Headers") else {}
        logger.info("=== Headers recibidos ===")
        for k, v in headers.items():
            logger.info(f"{k}: {v}")

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

        # 1️⃣ Configuración de OCI
        logger.info("Cargando configuración OCI desde 'config.oci'...")
        file_config = oci.config.from_file("config.oci")
        logger.info(f"Configuración cargada para el tenant: {file_config.get('tenancy')}")

        # 2️⃣ Obtener endpoint de la Queue
        logger.info(f"Obteniendo endpoint de la Queue con OCID: {QUEUE_OCID}")
        admin = oci.queue.QueueAdminClient(config=file_config)
        q = admin.get_queue(QUEUE_OCID).data
        messages_endpoint = q.messages_endpoint
        logger.info(f"Endpoint de mensajes: {messages_endpoint}")

        # 3️⃣ Crear cliente de Queue
        queue_client = oci.queue.QueueClient(config=file_config)
        queue_client.base_client.endpoint = messages_endpoint
        logger.info("Cliente de Queue configurado correctamente.")

        # 4️⃣ Preparar mensaje
        enriched_body = {"payload": body}
        logger.info(f"Mensaje a encolar: {json.dumps(enriched_body)}")

        put_details = oci.queue.models.PutMessagesDetails(
            messages=[
                oci.queue.models.PutMessagesDetailsEntry(
                    content=json.dumps(enriched_body)
                )
            ]
        )

        # 5️⃣ Enviar mensaje
        logger.info("Enviando mensaje a la Queue...")
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
                "message": "Mensaje encolado correctamente"
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
