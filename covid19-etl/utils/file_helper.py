import requests


def upload_file(path, url):
    with open(path, "rb") as data:
        try:
            r = requests.put(url, data=data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)
        finally:
            return r.status_code


class FileHelper:
    def __init__(self, base_url, program_name, project_code, access_token):
        self.base_url = base_url
        self.program_name = program_name
        self.project_code = project_code
        self.headers = {"Authorization": f"Bearer {access_token}"}

    def get_indexd_record(self, guid):
        url = f"{self.base_url}/index/index/{guid}"
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 404:
            return None
        else:
            r.raise_for_status()

    def indexd_record_exists(self, guid):
        record = self.get_indexd_record(guid)
        return True if record else False

    def find_by_name(self, filename):
        url = f"{self.base_url}/index/index?file_name={filename}"
        r = requests.get(url)
        data = r.json()
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

    def update_authz(self, did, rev):
        """Update authz field and remove uploader for did"""
        url = f"{self.base_url}/index/index/{did}?rev={rev}"
        body_json = {
            "authz": [f"/programs/{self.program_name}/projects/{self.project_code}"],
            "uploader": None,
        }
        try:
            r = requests.put(url, json=body_json, headers=self.headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(err)

    def get_presigned_url(self, filename):
        upload_url = f"{self.base_url}/user/data/upload"
        body_json = {"file_name": filename}
        r = requests.post(upload_url, json=body_json, headers=self.headers)
        data = r.json()
        return data["url"], data["guid"]

    def upload_file(self, path):
        basename = path.name
        presigned_url, guid = self.get_presigned_url(basename)
        upload_status = upload_file(path, presigned_url)
        if upload_status == requests.codes.ok:
            return guid
        return None
