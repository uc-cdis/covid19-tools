gender_mapping = {
    "Male": "gender_male",
    "Female": "gender_female",
    "Unknown/Left Blank": "gender_unknown_left_blank",
}

race_mapping = {
    "White": "race_white",
    "Hispanic": "race_hispanic",
    "Black": "race_black",
    "Left Blank": "race_left_blank",
    "Other": "race_other",
    "Asian": "race_asian",
    "NH/PI*": "race_nh_pi",
    "AI/AN**": "race_ai_an",
}

age_mapping = {
    "Unknown": "",
    "<20": "age_group_less_20",
    "20-29": "age_group_20_29",
    "30-39": "age_group_30_39",
    "40-49": "age_group_40_49",
    "50-59": "age_group_50_59",
    "60-69": "age_group_60_69",
    "70-79": "age_group_70_79",
    "80+": "age_group_greater_80",
}

fields_mapping = {
    "age": ("age_group", age_mapping),
    "gender": ("description", gender_mapping),
    "race": ("description", race_mapping),
}
