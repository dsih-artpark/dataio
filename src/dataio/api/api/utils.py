def user_has_preprocessed_access(user_permissions):
    for user_permission in user_permissions:
        if (
            user_permission.resource_type == "BUCKET"
            and user_permission.resource_id == "PREPROCESSED"
        ):
            return True
    return False


def user_has_dataset_download_access(user_permissions, dataset_id):
    for user_permission in user_permissions:
        if (
            user_permission.resource_type == "DATASET"
            and user_permission.resource_id == dataset_id
            and user_permission.permission == "DOWNLOAD"
        ):
            return True
    return False
