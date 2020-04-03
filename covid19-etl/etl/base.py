class BaseETL:
    def __init__(self, base_url, access_token):
        self.base_url = base_url
        self.access_token = access_token

    def files_to_submissions(self):
        pass

    def submit_metadata(self):
        pass
