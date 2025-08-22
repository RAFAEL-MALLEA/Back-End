import httpx
import time
from typing import List, Dict

def get_bsale(api_key: str, endpoint: str) -> List[Dict]:
    """
    Obtiene datos de la API de Bsale, manejando la paginación hasta que no haya más páginas.

    Args:
        api_key (str): La clave de API para autenticación.
        endpoint (str): El punto de entrada inicial de la API.

    Returns:
        List[Dict]: Una lista de diccionarios que contiene todos los datos de todas las páginas.
    """
    print(endpoint)
    url = f"https://api.bsale.io{endpoint}"
    headers = {
        "access_token": api_key,
        "Content-Type": "application/json"
    }

    all_data = []

    with httpx.Client() as client:
        while url:  # Continúa mientras 'url' no sea nulo
            for attempt in range(5):
                try:
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()

                    # Verifica si la respuesta contiene 'items' y extiende 'all_data' apropiadamente
                    if "items" in data:
                        all_data.extend(data["items"])
                    elif isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)

                    # Obtiene la siguiente URL para la paginación
                    url = data.get('next')  # Usa .get() para manejar el caso de que 'next' no exista

                    break  # Sale del bucle de reintentos si la solicitud tiene éxito

                except httpx.RequestError as exc:
                    print(f"Error en intento {attempt + 1}: {exc}")
                    if attempt < 4:
                        time.sleep(10)  # Espera antes de reintentar
                    else:
                        raise RuntimeError(f"Fallo tras 5 intentos al acceder a {url}")
            else:
                # Este bloque 'else' se ejecuta si el bucle 'for' termina normalmente,
                # es decir, si no se encontró un 'break'.  En este caso, significa que
                # todos los reintentos fallaron.  No es necesario aquí, pero se deja
                # como recordatorio de su propósito.
                pass

        return all_data