import requests


def get_api_key(base_url, access_token=None, headers=None):
    assert bool(access_token) ^ bool(
        headers
    ), "Must specify either 'access_token' or 'headers'"
    if not headers:
        headers = {"Authorization": f"Bearer {access_token}"}

    url = f"{base_url}/user/credentials/api"
    r = requests.post(url, json={"scope": ["openid", "user", "data"]}, headers=headers)
    assert (
        r.status_code == 200 and "api_key" in r.json()
    ), f"Could not get an API key from Fence ({r.status_code}):\n{r.text}"
    return r.json()["api_key"]


def get_access_token(base_url, api_key):
    url = f"{base_url}/user/credentials/api/access_token"
    r = requests.post(url, json={"api_key": api_key})
    assert (
        r.status_code == 200 and "access_token" in r.json()
    ), f"Could not get a new access token from Fence ({r.status_code}):\n{r.text}"
    return r.json()["access_token"]
