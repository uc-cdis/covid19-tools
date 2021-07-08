import requests
import time


def retry_wrapper(func):
    def retry_logic(*args, **kwargs):
        max_retries = kwargs.get("max_retries", 5)
        retries = 0
        sleep_sec = 0.1
        while retries < max_retries:
            if retries != 0:
                time.sleep(sleep_sec)
                sleep_sec *= 2
            try:
                resp = func(*args, **kwargs)
            except Exception as e:
                print(f"  Exception {e}. Retrying...")
                retries += 1
                if retries == max_retries:
                    raise
            else:
                if resp.status_code == 200:
                    return resp
                else:
                    print(f"  Status code {resp.status_code}. Retrying...")
                    retries += 1

    return retry_logic


class BaseETL:
    def __init__(self, base_url, access_token, s3_bucket):
        self.base_url = base_url
        self.access_token = access_token
        self.s3_bucket = s3_bucket

    def files_to_submissions(self):
        pass

    def submit_metadata(self):
        pass

    @retry_wrapper
    def get(self, path, *args, **kwargs):
        return requests.get(path, *args, **kwargs)
