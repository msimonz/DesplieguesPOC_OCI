import io
import os
import json
import base64
import requests

from fdk import response
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet

import oci  # SDK de Oracle para enviar a la cola


# ---------- Generador de PDF ----------
def crear_pdf_reportlab(salida_pdf: str, datos: dict):
    print("[crear_pdf_reportlab] Inicio")
    doc = SimpleDocTemplate(
        salida_pdf, pagesize=A4,
        leftMargin=40, rightMargin=40, topMargin=50, bottomMargin=40
    )
    estilos = getSampleStyleSheet()
    story = []

    story.append(Paragraph("Señores", estilos["Normal"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("ALIANZA FIDUCIARIA S.A.", estilos["Normal"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(datos.get("Ciudad", "Ciudad"), estilos["Normal"]))
    story.append(Spacer(1, 36))
    story.append(Paragraph("Ref.: " + datos.get("Referencia", "Referencia"), estilos["Normal"]))
    story.append(Spacer(1, 36))
    story.append(Paragraph("Respetados señores:", estilos["Normal"]))
    story.append(Spacer(1, 36))

    texto = (
        "En desarrollo del Plan de Pensiones Institucional ofrecido por la entidad patrocinadora con NIT "
        + datos.get("NIT", "")
        + " a favor de sus trabajadores o miembros nos permitimos relacionar a continuación los nombres de los partícipes a quienes se les nominarán los recursos descritos en el documento de adhesión al plan "
        +datos.get("Plan", "")
        + " de acuerdo con las condiciones de administración contenidas en el mismo."
    )
    story.append(Paragraph(texto, estilos["Normal"]))
    story.append(Spacer(1, 12))

    tabla_datos = [["CÉDULA", "NOMBRE", "ENCARGO"]]
    for cliente in datos.get("Clientes", []):
        fila = [cliente.get("cedula",""), cliente.get("nombre",""), cliente.get("encargo","")]
        tabla_datos.append(fila)

    col_widths = [120, 180, 180]
    tabla = Table(tabla_datos, colWidths=col_widths)
    tabla.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ]))
    story.append(tabla)
    story.append(Spacer(1, 24))
    story.append(Paragraph("Con la presente, suscribimos la obligación a cargo de la entidad patrocinadora a suministrar la relación de los partícipes cuando se realice un aporte o se presente una novedad de ingreso o de retiro de partícipes, caso en el cual se incluirá la justificación del retiro.", estilos["Normal"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("De igual manera, nos comprometemos a informar la cuenta y la entidad financiera para consignación o a retirar los recursos destinados al Plan, en caso de presentarse condición fallida sobre los aportes condicionados a favor de los partícipes, si para este evento se contempla la devolución de los saldos condicionados a favor de la patrocinadora.", estilos["Normal"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Agradecemos su atención.", estilos["Normal"]))
    story.append(Spacer(1, 36))
    story.append(Paragraph("Firma del Representante Legal,", estilos["Normal"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(datos.get("Representante", "Representante"), estilos["Normal"]))

    doc.build(story)
    print("[crear_pdf_reportlab] PDF generado con éxito")


# ---------- Publicar en OCI Queue ----------
# ---------- Publicar en OCI Queue ----------
def publish_to_queue(message: dict):
    queue_id = os.getenv("OCI_QUEUE_ID")
    if not queue_id:
        print("[queue] OCI_QUEUE_ID no configurado, no se publica")
        return

    try:
        signer = oci.auth.signers.get_resource_principals_signer()

        # Leer endpoint de variable de entorno (más flexible que hardcodear)
        queue_endpoint = os.getenv("OCI_QUEUE_ENDPOINT")

        # Cliente con endpoint correcto
        queue_client = oci.queue.QueueClient(
            config={}, 
            signer=signer, 
            service_endpoint=queue_endpoint
        )

        queue_client.put_messages(
            queue_id,
            put_messages_details=oci.queue.models.PutMessagesDetails(
                messages=[oci.queue.models.PutMessagesDetailsEntry(content=json.dumps(message))]
            )
        )
        return "[queue] Mensaje publicado con éxito"
    except Exception as e:
        return "[queue] Error publicando en la cola:", str(e)



# ---------- Handler ----------
def handler(ctx, data: io.BytesIO = None):
    try:
        raw = data.getvalue() if data else b"{}"
        payload = json.loads(raw.decode("utf-8") or "{}")

        out_path = "/tmp/salida.pdf"
        crear_pdf_reportlab(out_path, payload)

        with open(out_path, "rb") as f:
            pdf_b64 = base64.b64encode(f.read()).decode("utf-8")

        target_url = os.getenv("TARGET_API_URL")
        target_auth = os.getenv("TARGET_API_AUTH")

        if target_url:
            headers = {"Content-Type": "application/json"}
            if target_auth:
                headers["Authorization"] = target_auth

            payload_out = {"pdf_base64": pdf_b64, "metadata": payload}
            r = requests.post(target_url, headers=headers, json=payload_out, timeout=30)

            forward_status = r.status_code
            try:
                forward_body = r.json()
            except Exception:
                forward_body = r.text[:512]

            # Publicar en la cola el resultado del reenvío
            pqueue = publish_to_queue({"status": forward_status, "body": forward_body, "metadata": payload})

            if forward_status == 200:
                result = {"ok": True, "message": "Proceso completado con éxito", "Queue": pqueue, "pdf_base64": pdf_b64, "APIGW_receiver_response": r.json()}
                return response.Response(
                    ctx, status_code=200,
                    headers={"Content-Type": "application/json"},
                    response_data=json.dumps(result, ensure_ascii=False)
                )
            else:
                result = {"ok": False, "error": f"Fallo en el reenvío: {forward_status}", "body": forward_body, "Queue": pqueue, "pdf_base64": pdf_b64}
                return response.Response(
                    ctx, status_code=500,
                    headers={"Content-Type": "application/json"},
                    response_data=json.dumps(result, ensure_ascii=False)
                )
        else:
            return response.Response(
                ctx, status_code=200,
                headers={"Content-Type": "application/json"},
                response_data=json.dumps({"ok": True, "pdf_base64": pdf_b64, "Queue": pqueue}, ensure_ascii=False)
            )

    except Exception as e:
        return response.Response(
            ctx, status_code=500,
            headers={"Content-Type": "application/json"},
            response_data=json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
        )
