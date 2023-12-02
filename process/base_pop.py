from pandas import DataFrame
from numpy.random import choice
from pickle import dump as pickle_dump

from datetime import datetime
import ray


@ray.remote
def process_output_area_age_remote(output_area, age, df_gender_melt, df_ethnicity_melt):
    return process_output_area_age(output_area, age, df_gender_melt, df_ethnicity_melt)

def process_output_area_age(output_area, age, df_gender_melt, df_ethnicity_melt):
    population = []
    # Get the gender and ethnicity probabilities for the current output_area and age
    gender_probs = df_gender_melt.loc[
        (df_gender_melt["output_area"] == output_area) & (df_gender_melt["age"] == age),
        ["gender", "prob", "count"],
    ]
    ethnicity_probs = df_ethnicity_melt.loc[
        (df_ethnicity_melt["output_area"] == output_area)
        & (df_ethnicity_melt["age"] == age),
        ["ethnicity", "prob", "count"],
    ]

    # Determine the number of individuals for the current output_area and age
    n_individuals = int(gender_probs["count"].sum())

    if n_individuals == 0:
        return []

    # Randomly assign gender and ethnicity to each individual
    genders = choice(gender_probs["gender"], size=n_individuals, p=gender_probs["prob"])

    ethnicities = choice(
        ethnicity_probs["ethnicity"],
        size=n_individuals,
        p=ethnicity_probs["prob"],
    )

    for gender, ethnicity in zip(genders, ethnicities):
        individual = {
            "output_area": output_area,
            "age": age,
            "gender": gender,
            "ethnicity": ethnicity,
        }
        population.append(individual)

    return population


def create_base_pop(gender_data: DataFrame, ethnicity_data: DataFrame, output_area_filter: list or None, use_parallel: bool = False):


    if output_area_filter is not None:
        gender_data = gender_data[gender_data["output_area"].isin(output_area_filter)]
        ethnicity_data = ethnicity_data[ethnicity_data["output_area"].isin(output_area_filter)]

    # Assuming df_gender and df_ethnicity are your dataframes
    df_gender_melt = gender_data.melt(
        id_vars=["output_area", "gender"], var_name="age", value_name="count"
    )
    df_ethnicity_melt = ethnicity_data.melt(
        id_vars=["output_area", "ethnicity"], var_name="age", value_name="count"
    )

    # Normalize the data
    df_gender_melt["prob"] = df_gender_melt.groupby(["output_area", "age"])[
        "count"
    ].transform(lambda x: x / x.sum())
    df_ethnicity_melt["prob"] = df_ethnicity_melt.groupby(["output_area", "age"])[
        "count"
    ].transform(lambda x: x / x.sum())

    # Create synthetic population

    start_time = datetime.utcnow()

    if use_parallel:
        ray.init(num_cpus=16, include_dashboard=False)

    results = []

    output_areas = list(df_gender_melt["output_area"].unique())
    total_output_area = len(output_areas)
    for i, output_area in enumerate(output_areas):
        print(f"Processing: {i}/{total_output_area}")
        for age in df_gender_melt["age"].unique():
            if use_parallel:
                result = process_output_area_age_remote.remote(output_area, age, df_gender_melt, df_ethnicity_melt)
            else:
                result = process_output_area_age(
                    output_area, age, df_gender_melt, df_ethnicity_melt
                )
            results.append(result)

    if use_parallel:
        results = ray.get(results)

    population = [item for sublist in results for item in sublist]

    end_time = datetime.utcnow()
    total_mins = (end_time - start_time).total_seconds() / 60.0
    print(f"Processing time: {total_mins}")

    # Convert the population to a DataFrame
    return DataFrame(population)