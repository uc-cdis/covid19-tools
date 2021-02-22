import json
import requests
import pandas as pd
import shutil
import os

url = "https://chicagoland.pandemicresponsecommons.org/"


def get_wts_endpoint(namespace=os.getenv("NAMESPACE", "default")):
    return "http://workspace-token-service.{}.svc.cluster.local".format(namespace)


def get_access_token_from_wts(namespace=os.getenv("NAMESPACE", "default"), idp=None):
    """
    Try to fetch an access token for the given idp from the wts
    in the given namespace
    """
    # attempt to get a token from the workspace-token-service
    auth_url = get_wts_endpoint(namespace) + "/token/"
    if idp:
        auth_url += "?idp={}".format(idp)
    resp = requests.get(auth_url)
    return _handle_access_token_response(resp, "token")


def _handle_access_token_response(resp, token_key):
    """
    Shared helper for both get_access_token_with_key and get_access_token_from_wts
    """
    err_msg = "Failed to get an access token from {}:\n{}"
    if resp.status_code != 200:
        raise Exception(err_msg.format(resp.url, resp.text))
    try:
        json_resp = resp.json()
        return json_resp[token_key]
    except ValueError:  # cannot parse JSON
        raise Exception(err_msg.format(resp.url, resp.text))
    except KeyError:  # no access_token in JSON response
        raise Exception(err_msg.format(resp.url, json_resp))


def query_api(query_txt, variables=None):
    """ Request results for a specific query """
    if variables == None:
        query = {"query": query_txt}
    else:
        query = {"query": query_txt, "variables": variables}

    request_url = url + "api/v0/submission/graphql"
    r = requests.post(
        request_url,
        headers={"Authorization": "bearer " + get_access_token_from_wts()},
        json=query,
    )
    assert r.status_code == 200, r.text + "\n" + str(r.status_code)
    try:
        data = r.json()
    except ValueError as e:
        print(r.text)
        raise (e)

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
    assert "data" in data, data

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
        download_url, headers={"Authorization": "bearer " + get_access_token_from_wts()}
    )
    assert r.status_code == 200, r.text + "\n" + str(r.status_code)
    try:
        data = r.json()
    except ValueError as e:
        print(r.text)
        raise (e)
    assert "url" in data, data
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
