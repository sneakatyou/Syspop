from datetime import datetime
from logging import getLogger
from pickle import dump as pickle_dump

import ray
from numpy.random import choice
from pandas import DataFrame
import numpy as np
import pandas as pd
import random
logger = getLogger()

def get_index(value, age_ranges):
    try:
        return age_ranges.index(value)
    except ValueError:
        return "Value not found in the list"

@ray.remote
def create_base_pop_remote(area_data,input_mapping, output_area,age):
    return create_base_pop(area_data,input_mapping, output_area,age)


def create_base_pop(area_data,input_mapping, output_area,age):
    population = []
    # number_of_individuals = area_data[number_of_individuals]
    number_of_individuals = 100
    if number_of_individuals == 0:
        return []
    
    # gender_prob = area_data['age_gender_prob'][age]
    # Randomly assign gender and ethnicity to each individual
    age_index = get_index(age, input_mapping['age'])
    # male_prob = area_data['age_gender_prob'][age_index]
    # female_prob = area_data['age_gender_prob'][age_index+1].item() #TODO: Change it to be generalised
    # gender_prob = [male_prob,female_prob]
    gender_prob = [0.5,0.5]
    genders = choice(input_mapping['gender'], size=number_of_individuals, p=gender_prob)

    ethnicities = choice(
        input_mapping['race'],
        size=number_of_individuals,
        p=area_data["race_prob"],
    )

    for gender, ethnicity in zip(genders, ethnicities):
        individual = {
            "area": output_area,
            "age": age,
            "gender": gender,
            "ethnicity": ethnicity,
        }
        population.append(individual)

    return population


def base_pop_wrapper(
    input_data: dict,
    input_mapping: dict,
    use_parallel: bool = False,
    n_cpu: int = 8,
) -> DataFrame:
    """Create base population

    Args:
        gender_data (DataFrame): Gender data for each age
        ethnicity_data (DataFrame): Ethnicity data for each age
        output_area_filter (list or None): With area ID to be used
        use_parallel (bool, optional): If apply ray parallel processing. Defaults to False.

    Returns:
        DataFrame: Produced base population
    """
    start_time = datetime.utcnow()

    if use_parallel:
        ray.init(num_cpus=n_cpu, include_dashboard=False)

    results = []

    output_areas = list(input_data.keys())
    total_output_area = len(output_areas)
    for i, output_area in enumerate(output_areas):
        logger.info(f"Processing: {i}/{total_output_area}")
        for age in input_mapping['age']:
            if use_parallel:
                result = create_base_pop_remote.remote(
                    input_data[output_area], output_area,age,input_mapping
                )
            else:
                result = create_base_pop(
                    area_data=input_data[output_area], output_area=output_area,age=age,input_mapping=input_mapping
                )
            results.append(result)

    if use_parallel:
        results = ray.get(results)
        ray.shutdown()

    population = [item for sublist in results for item in sublist]

    end_time = datetime.utcnow()
    total_mins = (end_time - start_time).total_seconds() / 60.0

    # create an empty address dataset
    base_address = DataFrame(columns=["type", "name", "latitude", "longitude"])

    logger.info(f"Processing time (base population): {total_mins}")

    # Convert the population to a DataFrame
    return DataFrame(population), base_address

if __name__ == "__main__":
    
    output_dir = "/tmp/syspop_test/NYC/1"
    file = np.load("/Users/shashankkumar/Documents/AgentTorch_Official/AgentTorch/AgentTorch/helpers/census_data/nyc/generate_data/all_nta_agents.npy", allow_pickle=True)
    file_dict = file.item()
    
    input_mapping = {
    'race': ['hispanic', 'white', 'black', 'native', 'other', 'asian'],
    'age': ['U19', '20t29', '30t39', '40t49', '50t64', '65A'],
    'gender': ['male', 'female']
    }
    
    base_population = base_pop_wrapper(input_data=file_dict,input_mapping=input_mapping)
    
    