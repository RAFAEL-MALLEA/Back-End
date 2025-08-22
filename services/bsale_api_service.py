import requests
from typing import List, Dict, Any, Optional

BSALE_API_URL = "https://api.bsale.io/v1"

def get_bsale_data(api_key: str, endpoint: str) -> Optional[Dict | List]:
    """Función genérica para hacer llamadas GET a la API de Bsale."""
    headers = {
        'access_token': api_key,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(f"{BSALE_API_URL}{endpoint}", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al endpoint de Bsale '{endpoint}': {e}")
        return None

def get_bsale_users(api_key: str) -> Optional[List[Dict[str, Any]]]:
    """Obtiene la lista de usuarios de Bsale."""
    data = get_bsale_data(api_key, "/users.json?limit=100")
    return data.get("items") if data else None