import httpx
from datetime import datetime


async def get_data(api_key: str):
    url = f"https://credential.bsale.io/v1/instances/basic/{api_key}.json"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)

        if response.status_code == 200:
            data = response.json()
            trial_end_timestamp = data.get("trialEnd")
            if trial_end_timestamp:
                trial_end_date = datetime.utcfromtimestamp(trial_end_timestamp).strftime('%Y-%m-%d')
                data["trialEnd"] = trial_end_date

            return data

        elif response.status_code == 404:
            return None
        else:
            response.raise_for_status()