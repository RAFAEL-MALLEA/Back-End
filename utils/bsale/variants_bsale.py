import httpx

def get_all_variants(api_key: str):
    url = "https://api.bsale.io/v1/variants.json"
    headers = {
        "access_token": api_key,
        "Content-Type": "application/json"
    }

    all_variants = []

    with httpx.Client() as client:
        while url:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            all_variants.extend(items)

            url = data.get("next")

    return all_variants