# export PYTHONPATH=/home/zhangs/Github/Syspop/syspop

from pickle import load as pickle_load

from syspop import create as syspop_create
from syspop import diary as syspop_diary
from syspop import validate as syspop_validate
from syspop import vis as syspop_vis
import numpy as np
import pandas as pd



with open("/Users/shashankkumar/Documents/GitHub/Syspop/census_data/nyc/data/ethnicity.pkl", "rb") as fid:
    eth_data = pickle_load(fid)

with open("/Users/shashankkumar/Documents/GitHub/Syspop/census_data/nyc/data/gender.pkl", "rb") as fid:
    ag_data = pickle_load(fid)

with open("/Users/shashankkumar/Documents/GitHub/Syspop/census_data/nyc/data/geo.pkl", "rb") as fid:
    geog_data = pickle_load(fid)

with open("/Users/shashankkumar/Documents/GitHub/Syspop/census_data/nyc/data/housing.pkl", "rb") as fid:
    household_data = pickle_load(fid)

# with open("etc/data/test_data/commute.pickle", "rb") as fid:
#     commute_data = pickle_load(fid)

# with open("etc/data/test_data/work.pickle", "rb") as fid:
#     work_data = pickle_load(fid)

# with open("etc/data/test_data/school.pickle", "rb") as fid:
#     school_data = pickle_load(fid)

# with open("etc/data/test_data/hospital.pickle", "rb") as fid:
#     hospital_data = pickle_load(fid)

# with open("etc/data/test_data/supermarket.pickle", "rb") as fid:
#     supermarket_data = pickle_load(fid)

# with open("etc/data/test_data/restaurant.pickle", "rb") as fid:
#     restaurant_data = pickle_load(fid)

# with open("etc/data/test_data/pharmacy.pickle", "rb") as fid:
#     pharmacy_data = pickle_load(fid)


output_dir = "/tmp/syspop_test/NYC"
syn_areas = geog_data['area'].tolist()
# syn_areas = [135400, 111400, 110400]
# syn_areas = [135400]
# syn_areas = list(
#     geog_data["hierarchy"][geog_data["hierarchy"]["region"] == "Auckland"]["area"]
# )
# syn_areas = [135400, 111400, 110400]


if_run_syspop_create = True
if_run_diary = True
if_run_validation = True
if_run_vis = True


if if_run_syspop_create:
    syspop_create(
        syn_areas=syn_areas,
        output_dir=output_dir,
        pop_gender=ag_data,
        pop_ethnicity=eth_data,
        geo_hierarchy=geog_data,
        household=household_data,
        rewrite_base_pop=True,
        use_parallel=True,
        ncpu=8,
    )

if if_run_diary:
    syspop_diary(output_dir=output_dir, activities_cfg=None)

if if_run_validation:
    syspop_validate(
        output_dir=output_dir,
        pop_gender=pop_data["gender"],
        pop_ethnicity=pop_data["ethnicity"],
        household=household_data["household"],
        work_data=work_data,
        home_to_work=commute_data["home_to_work"],
    )

if if_run_vis:
    syspop_vis(
        output_dir=output_dir,
        plot_distribution=True,
        plot_travel=True,
        plot_location=True,
    )
