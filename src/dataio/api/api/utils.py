from dataio.api.database.functions import get_dataset
from dataio.api.database.models import AccessLevel


def user_has_preprocessed_access(user_permissions):
    for user_permission in user_permissions:
        if (
            user_permission.resource_type == "BUCKET"
            and user_permission.resource_id == "PREPROCESSED"
        ) or check_for_global_permission(user_permission):
            return True
    return False


def user_has_dataset_download_access(user_permissions, dataset_id):
    # check if dataset is public download
    dataset = get_dataset(dataset_id)
    if dataset.access_level == AccessLevel.DOWNLOAD:
        return True

    for user_permission in user_permissions:
        print(user_permission.__dict__)
        if (
            user_permission.resource_type == "DATASET"
            and user_permission.resource_id == dataset_id
            and user_permission.permission == "DOWNLOAD"
        ) or check_for_global_permission(user_permission):
            return True
    return False


def check_for_global_permission(user_permission):
    if (
        user_permission.resource_type == "*"
        and user_permission.resource_id == "*"
        and user_permission.permission == "DOWNLOAD"
    ):
        return True
