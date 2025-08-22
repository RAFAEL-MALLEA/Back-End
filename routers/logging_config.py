import logging
from logging.handlers import RotatingFileHandler

# Configurar el logger
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Usar RotatingFileHandler para limitar el tamaño de los archivos de log
# Esto creará hasta 5 archivos de log de 5MB cada uno.
log_handler = RotatingFileHandler('webhook_logs.txt', maxBytes=5*1024*1024, backupCount=5)
log_handler.setFormatter(log_formatter)

# Obtener el logger y añadir el handler
logger = logging.getLogger('webhook_logger')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)