from dataio.api.database.enums import TemporalResolution

# Free-text temporalResolution values seen in metadata.yaml, mapped onto the
# DB's TemporalResolution enum. Several of these (quinquennial, decadal, ...)
# collapse lossily into YEAR because the enum has no coarser-than-YEAR
# member - callers must record the original text in `comments` when that
# happens, rather than silently discarding it. See metadata-architecture
# memo, Limitations -> "temporal_resolution has no mapping table".
TEMPORAL_RESOLUTION_MAP: dict[str, TemporalResolution] = {
    "year": TemporalResolution.YEAR,
    "yearly": TemporalResolution.YEAR,
    "annual": TemporalResolution.YEAR,
    "annually": TemporalResolution.YEAR,
    "quinquennial": TemporalResolution.YEAR,
    "quinquennium": TemporalResolution.YEAR,
    "5-year": TemporalResolution.YEAR,
    "5-yearly": TemporalResolution.YEAR,
    "decadal": TemporalResolution.YEAR,
    "decennial": TemporalResolution.YEAR,
    "month": TemporalResolution.MONTH,
    "monthly": TemporalResolution.MONTH,
    "week": TemporalResolution.WEEK,
    "weekly": TemporalResolution.WEEK,
    "day": TemporalResolution.DATE,
    "daily": TemporalResolution.DATE,
    "date": TemporalResolution.DATE,
    "hour": TemporalResolution.HOUR,
    "hourly": TemporalResolution.HOUR,
    "minute": TemporalResolution.MINUTE,
    "second": TemporalResolution.SECOND,
    "none": TemporalResolution.NONE,
    "static": TemporalResolution.NONE,
    "n/a": TemporalResolution.NONE,
}

# Values that map onto a coarser enum member than what they actually mean -
# resolve_temporal_resolution's caller should note the original text
# somewhere (e.g. metadata comments) when the resolved key is in here.
LOSSY_TEMPORAL_RESOLUTIONS = {
    "quinquennial", "quinquennium", "5-year", "5-yearly", "decadal", "decennial",
}


def resolve_temporal_resolution(raw: str) -> str:
    """Map free-text temporalResolution onto a valid TemporalResolution enum
    value. Raises instead of the old str(...).upper() behavior, which
    silently coerced anything (e.g. "quinquennial" -> the invalid literal
    "QUINQUENNIAL") without ever checking it against the real enum.
    """
    key = str(raw or "").strip().lower()
    mapped = TEMPORAL_RESOLUTION_MAP.get(key)
    if mapped is None:
        valid_values = [e.value for e in TemporalResolution]
        raise ValueError(
            f"Unrecognized temporalResolution '{raw}'. Extend "
            f"TEMPORAL_RESOLUTION_MAP in {__name__}, or fix the source "
            f"metadata.yaml. Valid enum values: {valid_values}"
        )
    return mapped.value


def is_lossy_temporal_resolution(raw: str) -> bool:
    """Whether `raw` collapses into a coarser enum member than it actually
    means (e.g. "quinquennial" -> YEAR) - callers should preserve the
    original text (e.g. in a comments field) when this is True.
    """
    return str(raw or "").strip().lower() in LOSSY_TEMPORAL_RESOLUTIONS
