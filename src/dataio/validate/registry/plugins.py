from dataio.validate.contracts.models import DatasetKind
from dataio.validate.validators.base import ValidatorPlugin
from dataio.validate.validators.geojson import GeoJSONValidator
from dataio.validate.validators.tabular import TabularValidator


def get_validator_plugin(dataset_kind: DatasetKind) -> ValidatorPlugin:
    if dataset_kind == DatasetKind.GEOJSON:
        return GeoJSONValidator()
    return TabularValidator()
