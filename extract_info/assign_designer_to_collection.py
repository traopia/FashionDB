import pandas as pd
import re 

import ahocorasick  # pip install pyahocorasick




def extract_birth_year(text, min_year=1850, max_year=2025):
    if not isinstance(text, str):
        return None
    years = re.findall(r"\b(1[89]\d{2}|20\d{2})\b", text)  # captures 1800–2099
    years = [int(y) for y in years if min_year <= int(y) <= max_year]
    return min(years) if years else None


def propagate_single(row, df):
    fh, yr = row.fashion_house, row.year
    current_names = row.designer_names

    # Helper to compare designer lists ignoring order
    def same_names(list1, list2):
        return set(list1) == set(list2)

    # If multiple designers but more than 2, treat as ambiguous and propagate
    if len(current_names) > 2:
        current_names = []

    # If multiple designers with 2 or fewer names,
    # check if exact same list appears in neighbor years
    elif 1 < len(current_names) <= 2:
        neighbors = df.loc[
            (df.fashion_house == fh) &
            (df.year.isin([yr - 3, yr, yr + 3])) &
            (df.index != row.name)
        ]
        # Check if any neighbor has exactly the same designer list (order ignored)
        repeated = neighbors["designer_names"].apply(
            lambda x: len(x) == len(current_names) and same_names(x, current_names)
        ).any()
        if not repeated:
            current_names = []

    # If single designer but appears only once in the whole fashion house, treat as empty
    if len(current_names) == 1:
        name = current_names[0]
        count_in_house = sum(
            len(names) == 1 and names[0] == name
            for names in df.loc[df.fashion_house == fh, "designer_names"]
        )
        if count_in_house == 1:
            current_names = []

    # If empty now, try to propagate from neighbors with exactly one designer
    if len(current_names) == 0:
        neighbors = df.loc[
            (df.fashion_house == fh) &
            (df.year.isin([yr - 2, yr, yr + 2])) &
            (df.index != row.name)
        ]
        single_neighbors = neighbors[neighbors["designer_names"].apply(lambda x: len(x) == 1)]

        if not single_neighbors.empty:
            single_neighbors = single_neighbors.iloc[
                (single_neighbors["year"] - yr).abs().argsort()
            ]
            return single_neighbors["designer_names"].iloc[0]

    return current_names

def fill_empty_designer_names(df):
    # Ensure sorted by fashion_house and year
    df = df.sort_values(["fashion_house", "year"]).reset_index(drop=True)

    # Create a copy of the designer_names column to modify
    filled = df["designer_name"].copy()

    # For each fashion house, fill empty designer_names from closest year
    for fh, group in df.groupby("fashion_house"):
        years = group["year"].values
        names = group["designer_name"].values

        # Indices of rows in original df for this fashion house
        indices = group.index.values

        # For each empty designer_names, find closest non-empty
        for i, name_list in zip(indices, names):
            if len(name_list) == 0:
                year = df.at[i, "year"]

                # Find candidates with non-empty designer_names
                non_empty = [(idx, abs(year - df.at[idx, "year"])) 
                             for idx in indices if len(df.at[idx, "designer_name"]) > 0]

                if non_empty:
                    # Pick index with minimum year difference
                    closest_idx = min(non_empty, key=lambda x: x[1])[0]

                    # Assign that designer_names list
                    filled.at[i] = df.at[closest_idx, "designer_name"]

    df["designer_name"] = filled
    return df


if __name__ == "__main__":
    #designer lists
    bio_designers = pd.read_json("data/designer_data_fmd.json", lines=True)
    bio_designers["year_birth"] = bio_designers["biography"].apply(extract_birth_year)
    designers_fmd = bio_designers[bio_designers["year_birth"]>1910].designer_name
    designer_bof = pd.read_json("data/all_designer_data_BOF.json", lines=True).designer_name
    additional_designers = pd.read_csv("data/names/additional_designers.csv").designer_name
    all_designers = set(list(designers_fmd )+ list(designer_bof )+ list(additional_designers))

    #df
    df = pd.read_parquet("data/vogue_data.parquet")
    df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 10]


    # Build automaton
    A = ahocorasick.Automaton()
    for name in all_designers:
        A.add_word(name.lower(), name)
    A.make_automaton()

    def find_names(text):
        matches = set()
        for end_idx, orig_name in A.iter(text.lower()):
            matches.add(orig_name)
        return list(matches)

    df = df.dropna(subset= ["description"])
    df["designer_names"] = df["description"].apply(find_names)


    df["year"] = df["year"].astype(int)
    # Sort to make shifting easier
    df = df.sort_values(["fashion_house", "year"]).reset_index(drop=True)
    # Apply propagation
    df["designer_name"] = df.apply(lambda r: propagate_single(r, df), axis=1)

    df = fill_empty_designer_names(df)
    df.to_parquet("data/vogue_data.parquet")