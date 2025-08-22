from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List
import boto3
from botocore.exceptions import ClientError
from core.config import settings

class EmailError(Exception):
    pass

def send_email(to_addresses: List[str], subject: str, body_html: str) -> bool:
    """
    Envía un email a una lista de destinatarios usando AWS SES.
    """
    ses_client = boto3.client("ses", region_name=settings.AWS_REGION_NAME)
    
    if not to_addresses:
        print("Error: No se proporcionaron destinatarios para el email.")
        return False
    
    try:
        response = ses_client.send_email(
            Destination={'ToAddresses': to_addresses},
            Message={
                'Body': {'Html': {'Charset': "UTF-8", 'Data': body_html}},
                'Subject': {'Charset': "UTF-8", 'Data': subject},
            },
            Source=settings.SENDER_EMAIL,
        )
    except ClientError as e:
        print(f"Error al enviar email a {', '.join(to_addresses)}: {e.response['Error']['Message']}")
        raise EmailError(f"Fallo al enviar email con AWS SES: {e}")
    
    print(f"Email enviado exitosamente a {', '.join(to_addresses)}. Message ID: {response['MessageId']}")
    return True

def send_email_with_attachments(
    to_addresses: List[str], 
    subject: str, 
    body_html: str, 
    attachments: List[Dict[str, Any]]
) -> bool:
    """
    Envía un email con archivos adjuntos usando AWS SES.
    Cada adjunto debe ser un diccionario: {'filename': str, 'data': BytesIO_object}
    """
    ses_client = boto3.client("ses", region_name=settings.AWS_REGION_NAME)
    
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = settings.SENDER_EMAIL
    msg['To'] = ", ".join(to_addresses)

    # Crear el cuerpo del mensaje
    msg_body = MIMEMultipart('alternative')
    html_part = MIMEText(body_html.encode('utf-8'), 'html', 'utf-8')
    msg_body.attach(html_part)
    msg.attach(msg_body)

    # Añadir los archivos adjuntos
    for attachment in attachments:
        try:
            part = MIMEApplication(attachment['data'].getvalue())
            part.add_header('Content-Disposition', 'attachment', filename=attachment['filename'])
            msg.attach(part)
        except Exception as e:
            print(f"Error al adjuntar el archivo {attachment['filename']}: {e}")
            continue # Salta este archivo y continúa con los demás

    try:
        response = ses_client.send_raw_email(
            Source=settings.SENDER_EMAIL,
            Destinations=to_addresses,
            RawMessage={'Data': msg.as_string()}
        )
    except ClientError as e:
        print(f"Error al enviar email con adjuntos: {e.response['Error']['Message']}")
        raise EmailError(f"Fallo al enviar email con AWS SES: {e}")
    
    print(f"Email con adjuntos enviado exitosamente a {', '.join(to_addresses)}. Message ID: {response['MessageId']}")
    return True