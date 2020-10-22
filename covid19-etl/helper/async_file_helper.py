import asyncio
from aiohttp import ClientSession
import requests


async def async_upload_file(path, url):
    with open(path, "rb") as data:
        try:
            async with ClientSession() as session:
                async with session.put(url, data=data) as r:
                    return r.status
        except Exception as err:
            print(err)


class AsyncFileHelper:
    def __init__(self, base_url, program_name, project_code, access_token):
        self.base_url = base_url
        self.program_name = program_name
        self.project_code = project_code
        self.headers = {"Authorization": f"Bearer {access_token}"}

    async def async_find_by_name(self, filename):
        url = f"{self.base_url}/index/index?file_name={filename}"
        async with ClientSession() as session:
            async with session.get(url) as r:
                r.raise_for_status()
                data = await r.json()
                if data["records"]:
                    assert (
                        len(data["records"]) == 1
                    ), f"multiple records for filename, something wrong: {filename}"
                    did = data["records"][0]["did"]
                    rev = data["records"][0]["rev"]
                    md5sum = data["records"][0]["hashes"]["md5"]
                    size = data["records"][0]["size"]
                    return did, rev, md5sum, size
                return None, None, None, None

    async def async_update_authz(self, did, rev):
        url = f"{self.base_url}/index/index/{did}?rev={rev}"
        async with ClientSession() as session:
            async with session.put(
                url,
                json={
                    "authz": [
                        f"/programs/{self.program_name}/projects/{self.project_code}"
                    ],
                    "uploader": None,
                },
            ) as r:
                r.raise_for_status()

    async def async_get_presigned_url(self, filename):
        upload_url = f"{self.base_url}/user/data/upload"
        body_json = {"file_name": filename}
        async with ClientSession() as session:
            async with session.post(
                upload_url, json=body_json, headers=self.headers
            ) as res:
                res.raise_for_status()
                data = await res.json()
                return data["url"], data["guid"]

    async def async_upload_file(self, path):
        basename = path.name
        presigned_url, guid = await self.async_get_presigned_url(basename)
        upload_status = await async_upload_file(path, presigned_url)
        if upload_status == requests.codes.ok:
            return guid
        return None
