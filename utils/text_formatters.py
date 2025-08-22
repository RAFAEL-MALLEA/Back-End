import re
import unicodedata

def slugify_for_aws(text: str) -> str:
    """
    Convierte un string en un formato seguro para recursos de AWS como 
    identificadores de RDS, que no permiten caracteres especiales ni acentos.
    """
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    return text[:60]