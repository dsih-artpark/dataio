def extract_url(tree_list: list) -> dict:
    """Create a dictionary with key = path and value = url from a json tree

    Args:
        tree_list (list): list of dictionaries

    Returns:
        dict: dict with key = path, and value = url
    """
    tree_dict = {}

    for tree in tree_list:
        tree_dict[tree["path"]] = tree["url"]

    return tree_dict
