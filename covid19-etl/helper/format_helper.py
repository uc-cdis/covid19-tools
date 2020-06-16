import datetime
import re


def format_submitter_id(node, args):
    """
    Generates "submitter_id" for node with additional identificator values.
    Resulting "submitter_id" only contains lowercase letters, digits, underscore and dash.

    Args:
        node (str): node name for "submitter_id"
        args (dict): additional arguments to add to "submitter_id"

    Returns:
        str: generated "submitter_id"
    """
    submitter_id = node
    for v in args.values():
        submitter_id += "_{}".format(v)

    submitter_id = submitter_id.lower()
    submitter_id = re.sub("[^a-z0-9-_]+", "-", submitter_id)

    return submitter_id


def derived_submitter_id(submitter_id, original_node, derived_node, args):
    """
    Derive "submitter_id" for other node.

    Args:
        submitter_id (str): "submitter_id" to derive from
        original_node (str): name of original node
        derived_node (str): name of derived node
        args (dict): additional arguments to add to "derived_submitter_id"

    Returns:
        str: generated "derived_submitter_id"
    """
    derived_submitter_id = submitter_id.replace(original_node, derived_node)
    for v in args.values():
        derived_submitter_id += "_{}".format(v)
    return derived_submitter_id


def idph_get_date(date_json):
    """
    Get date from IDPH JSON

    Args:
        date_json (dict): JSON date with "year", "month", "date" fields

    Returns:
        str: datetime in "%Y-%m-%d" format
    """
    date = datetime.date(**date_json)
    return date.strftime("%Y-%m-%d")
