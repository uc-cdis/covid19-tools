from aiohttp import ClientSession
import requests


class AsyncFileHelper:
    """Asynchronous file helper class"""

    session = None

    def __init__(self, base_url, program_name, project_code, access_token):
        self.base_url = base_url
        self.program_name = program_name
        self.project_code = project_code
        self.headers = {"Authorization": f"Bearer {access_token}"}

    @classmethod
    def get_session(cls):
        if cls.session is None:
            cls.session = ClientSession()
        return cls.session

    @classmethod
    def close_session(cls):
        if cls.session is not None:
            return cls.session.close()

    async def async_get_indexd_record(self, guid):
        url = f"{self.base_url}/index/index/{guid}"
        session = AsyncFileHelper.get_session()
        async with session.get(url) as r:
            if r.status == 200:
                return r.json()
            elif r.status == 404:
                return None
            else:
                r.raise_for_status()

    async def async_indexd_record_exists(self, guid):
        record = await self.async_get_indexd_record(guid)
        return True if record else False

    async def async_find_by_name(self, filename):
        """Asynchronous call to fine the indexd record given a filename"""

        url = f"{self.base_url}/index/index?file_name={filename}"
        session = AsyncFileHelper.get_session()
        async with session.get(url) as r:
            r.raise_for_status()
            data = await r.json()
            if data["records"]:
                for record in data["records"]:
                    if record.get("hashes", {}).get("md5"):
                        break
                did = record["did"]
                rev = record["rev"]
                md5sum = record.get("hashes", {}).get("md5", "")
                size = record["size"]
                authz = record["authz"]
                filename = record["file_name"]
                return did, rev, md5sum, size, filename, authz
            return None, None, None, None, "", None

    async def async_update_authz(self, did, rev):
        """Asynchronous update authz field and remove uploader for did"""

        url = f"{self.base_url}/index/index/{did}?rev={rev}"
        session = AsyncFileHelper.get_session()
        async with session.put(
            url,
            json={
                "authz": [
                    f"/programs/{self.program_name}/projects/{self.project_code}"
                ],
                "uploader": None,
            },
            headers=self.headers,
        ) as r:
            r.raise_for_status()

    async def async_get_presigned_url(self, filename):
        """Asynchronous call to get presigned url"""

        upload_url = f"{self.base_url}/user/data/upload"
        body_json = {"file_name": filename}
        session = AsyncFileHelper.get_session()
        async with session.post(
            upload_url, json=body_json, headers=self.headers
        ) as res:
            res.raise_for_status()
            data = await res.json()
            return data["url"], data["guid"]

    async def async_upload_file(self, path):
        """Asynchronous call to upload the file"""

        async def _async_upload_file(path, url):
            with open(path, "rb") as data:
                async with ClientSession() as session:
                    async with session.put(url, data=data) as r:
                        return r.status

        basename = path.name
        presigned_url, guid = await self.async_get_presigned_url(basename)
        upload_status = await _async_upload_file(path, presigned_url)
        if upload_status == requests.codes.ok:
            return guid
        return None

    async def async_index_record(self, did, size, filename, url, authz, md5):
        """Asynchronous update authz field for did"""

        session = AsyncFileHelper.get_session()
        async with session.post(
            f"{self.base_url}/index/index",
            json={
                "did": did,
                "form": "object",
                "size": size,
                "file_name": filename,
                "urls": [url],
                "authz": [authz],
                "hashes": {"md5": md5},
            },
            headers=self.headers,
        ) as r:
            r.raise_for_status()
