import math

import jaro
import pandas as pd

# matching algorithm: which columns
STRING_DISTANCE = [
    "family",
    "genus",
    "specificEpithet",
    "country",
    "recordedBy",
    "collectionCode",
    "catalogNumber",
]
INT_DISTANCE = ["year", "month", "individualCount"]
FLOAT_DISTANCE = ["decimalLongitude", "decimalLatitude", "elevation"]

MATCHED_FIELDS = STRING_DISTANCE + INT_DISTANCE + FLOAT_DISTANCE


def normalize_occurrence(occurrence):
    # remove unused data
    if isinstance(occurrence.get("elevation"), str):
        occurrence["elevation"] = occurrence["elevation"].replace("ca.", "")

    # ensure there is a value
    for key in STRING_DISTANCE:
        if key not in occurrence:
            occurrence[key] = ""
    for key in INT_DISTANCE:
        if key not in occurrence:
            occurrence[key] = None
    for key in FLOAT_DISTANCE:
        if key not in occurrence:
            occurrence[key] = None
        if occurrence[key] is not None and not isinstance(occurrence[key], float):
            occurrence[key] = float(occurrence[key])

    # normalize value
    for key in ["decimalLongitude", "decimalLatitude"]:
        if occurrence[key] in [360, 0]:
            occurrence[key] = None
    if occurrence.get("elevation", 0) and occurrence["elevation"] < -6000000:
        occurrence["elevation"] = None


def get_string_distance_for_column(matcit, specimen, column_name):
    candidate = specimen[column_name]
    ref_value = matcit[column_name]
    return jaro.jaro_winkler_metric(ref_value, candidate)


def get_numeric_distance_for_column(matcit, institution_occurrences, column_name):
    candidates = [o[column_name] for o in institution_occurrences]
    ref_value = matcit[column_name]
    value_for_max = [c for c in candidates if c is not None]
    if ref_value:
        value_for_max.append(ref_value)

    if len(value_for_max) == 0 or ref_value is None:
        return [None] * len(candidates)

    max_value = abs(max(value_for_max))

    if max_value == 0:
        # all values are either 0 or None
        return [1 if candidate is not None else None for candidate in candidates]

    return [1 - (abs(candidate - ref_value) / max_value) if candidate is not None else None for candidate in candidates]


def get_score(matcit, specimen):
    data = {}
    df = pd.DataFrame(data=data)
    specimen_list = [ specimen ]
    for key in STRING_DISTANCE:
        df[key] = [ get_string_distance_for_column(matcit, specimen, key) ]

    string_mean = df.mean(axis=1).to_list()

    for key in INT_DISTANCE:
        df[key] = get_numeric_distance_for_column(matcit, specimen_list, key)
    for key in FLOAT_DISTANCE:
        df[key] = get_numeric_distance_for_column(matcit, specimen_list, key)
    df["$mean"] = df.mean(axis=1).to_list()
    df["$string_mean"] = string_mean
    df = df.sort_values(by=["$string_mean", "$mean"])
    df = df.round(3)

    for _, row in df.iterrows():
        return {
            field: None if isinstance(score, float) and math.isnan(score) else score
            for field, score in row.to_dict().items()
            if field != '$index'
        }
