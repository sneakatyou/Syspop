

# export PYTHONPATH=~/Github/Syspop/
from process.base_pop import base_pop_wrapper
from process.utils import setup_logging
from process.household import household_wrapper
from process.social_economic import social_economic_wrapper
from process.address import add_address_wrapper
from process.work import work_and_commute_wrapper
from pickle import load as pickle_load
from pickle import dump as pickle_dump

with open("/tmp/syspop/population.pickle", "rb") as fid:
    pop_data = pickle_load(fid)

with open("/tmp/syspop/geography.pickle", "rb") as fid:
    geog_data = pickle_load(fid)

with open("/tmp/syspop/household.pickle", "rb") as fid:
    household_data = pickle_load(fid)

with open("/tmp/syspop/commute.pickle", "rb") as fid:
    commute_data = pickle_load(fid)

with open("/tmp/syspop/work.pickle", "rb") as fid:
    work_data = pickle_load(fid)

logger = setup_logging()

create_base_pop_flag = False
assign_household_flag = False
assign_socialeconomic_flag = False
assign_address_flag = False
assign_commute_flag = False
assign_work_flag = True


if create_base_pop_flag:
    synpop = base_pop_wrapper(
        pop_data["gender"], 
        pop_data["ethnicity"],
        list(geog_data["hierarchy"][geog_data["hierarchy"]["region"] == "Auckland"]["area"]),
        use_parallel=True,
        n_cpu=8)

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": synpop}, fid)

if assign_household_flag:
    with open("/tmp/synpop.pickle", "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = household_wrapper(
        household_data["household"], 
        base_pop["synpop"], 
        use_parallel=True, 
        n_cpu=16)

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_socialeconomic_flag:
    with open("/tmp/synpop.pickle", "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = social_economic_wrapper(
        base_pop["synpop"], 
        geog_data["socialeconomic"])

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_address_flag:
    with open("/tmp/synpop.pickle", "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = add_address_wrapper(
        base_pop["synpop"], 
        geog_data["address"],
        use_parallel=True)

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)


if assign_work_flag:
    with open("/tmp/synpop.pickle", "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = work_and_commute_wrapper(
        work_data,
        base_pop["synpop"], 
        commute_data,
        geog_data["hierarchy"],
        use_parallel=True)

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

with open("/tmp/synpop.pickle", "rb") as fid:
    synpop_data = pickle_load(fid)