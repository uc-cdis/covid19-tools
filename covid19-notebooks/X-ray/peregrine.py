import json
import requests
import pandas as pd
import shutil

url = "https://chicagoland.pandemicresponsecommons.org/"


def get_token():
    with open("/home/jovyan/pd/credentials.json", "r") as f:
        creds = json.load(f)
    token_url = url + "user/credentials/api/access_token"
    token = requests.post(token_url, json=creds).json()["access_token"]
    return token


def query_api(query_txt, variables=None):
    """ Request results for a specific query """
    if variables == None:
        query = {"query": query_txt}
    else:
        query = {"query": query_txt, "variables": variables}

    request_url = url + "api/v0/submission/graphql"
    output = requests.post(
        request_url, headers={"Authorization": "bearer " + get_token()}, json=query
    ).text
    data = json.loads(output)

    if "errors" in data:
        print(data)

    return data


def get_images(project_id):
    """ Get list of images for specific project"""

    query_txt = (
        """{ imaging_file(project_id: "%s",first:0){ object_id, clinical_notes,file_name }}"""
        % project_id
    )
    data = query_api(query_txt)

    images = []
    for image in data["data"]["imaging_file"]:
        images.append(
            [(image["object_id"], image["file_name"]), image["clinical_notes"]]
        )

    df = pd.DataFrame.from_records(images, columns=["GUID", "clinical_notes"])

    return df


def get_observation_images(project_id):
    """ Get list of images for specific project"""

    query_txt = (
        """{ subject(project_id: "%s", first:0, with_path_to: {type: "observation", pneumonia_type: "COVID-19"}){imaging_files(first:0, data_category:"X-Ray Image"){ object_id,file_name }}}"""
        % project_id
    )
    data = query_api(query_txt)

    images = []
    for image in data["data"]["subject"]:
        for file in image["imaging_files"]:
            images.append([(file["object_id"], file["file_name"])])

    df = pd.DataFrame.from_records(images, columns=["GUID"])

    return df


def download_object(guid):

    download_url = url + "user/data/download/" + guid[0]
    r = requests.get(
        download_url, headers={"Authorization": "bearer " + get_token()}
    ).text
    data = json.loads(r)
    image_url = data["url"]

    r = requests.get(image_url, stream=True)
    # Check if the image was retrieved successfully
    if r.status_code == 200:
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        r.raw.decode_content = True

        # Open a local file with wb ( write binary ) permission.
        with open(guid[1], "wb") as f:
            shutil.copyfileobj(r.raw, f)
    return image_url
