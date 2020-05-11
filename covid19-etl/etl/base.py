class BaseETL:
    def __init__(self, base_url, access_token, s3_bucket):
        self.base_url = base_url
        self.access_token = access_token
        self.s3_bucket = s3_bucket

    def files_to_submissions(self):
        pass

    def submit_metadata(self):
        pass
