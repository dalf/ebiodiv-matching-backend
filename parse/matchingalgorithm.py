import math
import pandas as pd
import batch_jaro_winkler as bjw

from config import STRING_DISTANCE, INT_DISTANCE, FLOAT_DISTANCE


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


def get_string_distance_for_column(matcit, institution_occurrences, column_name):
    candidates = [o[column_name] for o in institution_occurrences]
    ref_value = matcit[column_name]

    index = {}
    for i, c in enumerate(candidates):
        index.setdefault(c, []).append(i)
    exportable_model = bjw.build_exportable_model(list(index.keys()))
    runtime_model = bjw.build_runtime_model(exportable_model)
    res = bjw.jaro_winkler_distance(runtime_model, ref_value)

    result = [None] * len(candidates)
    for canditate, distance in res:
        for i in index[canditate]:
            result[i] = distance
    return result


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


def set_score(cluster):
    matcit = cluster["materialCitationOccurrence"]
    institution_occurrences = cluster["institutionOccurrences"]
    data = {}
    df = pd.DataFrame(data=data)
    for key in STRING_DISTANCE:
        df[key] = get_string_distance_for_column(matcit, institution_occurrences, key)

    string_mean = df.mean(axis=1).to_list()

    for key in INT_DISTANCE:
        df[key] = get_numeric_distance_for_column(matcit, institution_occurrences, key)
    for key in FLOAT_DISTANCE:
        df[key] = get_numeric_distance_for_column(matcit, institution_occurrences, key)
    df["$mean"] = df.mean(axis=1).to_list()
    df["$string_mean"] = string_mean
    df["$index"] = list(range(0, len(institution_occurrences)))
    df = df.sort_values(by=["$string_mean", "$mean"])
    df = df.round(3)

    for _, row in df.iterrows():
        occurrence = cluster["institutionOccurrences"][int(row["$index"])]
        occurrence["$score"] = {
            field: None if isinstance(score, float) and math.isnan(score) else score
            for field, score in row.to_dict().items()
        }
        del occurrence['$score']['$index']
    cluster["institutionOccurrences"].sort(key=lambda o: o.get("$score", {}).get("$string_mean", 0))


def matching_algorithm_cluster(data):
    normalize_occurrence(data["materialCitationOccurrence"])
    for occurrence in data["institutionOccurrences"]:
        normalize_occurrence(occurrence)
    set_score(data)
    return data
