import enum


class AccessLevel(str, enum.Enum):
    NONE = "NONE"
    VIEW = "VIEW"
    DOWNLOAD = "DOWNLOAD"


class SpatialResolution(str, enum.Enum):
    COUNTRY = "COUNTRY"
    STATE = "STATE"
    UT = "UT"
    DISTRICT = "DISTRICT"
    SUBDISTRICT = "SUBDISTRICT"
    MUNICIPALITY = "MUNICIPALITY"
    VILLAGE = "VILLAGE"
    WARD = "WARD"
    PRABHAG = "PRABHAG"
    ULB = "ULB"
    LAT_LONG = "LAT/LONG"
    OTHER = "OTHER"


class TemporalResolution(str, enum.Enum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    WEEK = "WEEK"
    DATE = "DATE"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    NONE = "NONE"


class VersionType(str, enum.Enum):
    PREPROCESSED = "PREPROCESSED"
    STANDARDISED = "STANDARDISED"


class UpdationFrequency(str, enum.Enum):
    ONE_TIME = "ONE_TIME"
    YEARLY = "YEARLY"
    MONTHLY = "MONTHLY"
    WEEKLY = "WEEKLY"
    DAILY = "DAILY"
    HOURLY = "HOURLY"
    REAL_TIME = "REAL_TIME"
    ADHOC = "ADHOC"


class ResourceType(str, enum.Enum):
    DATASET = "DATASET"
    GROUP = "GROUP"
    BUCKET = "BUCKET"
