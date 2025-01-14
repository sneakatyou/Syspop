

# Synthetic & Simulated Population (SysPop)

<p align="center">
    <img src="etc/wiki_img/agent2.gif?raw=true" alt="GIF Example" width="22%">
    <img src="etc/wiki_img/work_place.gif?raw=true" alt="Sample Image" width="40%">
</p>

_The above animations show: Left: the movements of 30 synthetic residents sampled from the area code 251400 (Wellington Central) in the early morning (between 7am and 8am). Right: Where people go to work in one day (3000 sampled synthetic citizens across Auckland region)_


**Syspop** is a package developed to simplify and modularize the synthesis of populations, enhancing efficiency and user-friendliness. The outputs generated by **Syspop** are independent of any downstream applications, such as agent-based models, making them versatile inputs for various modeling needs or standalone products. 

**The documentation for the package can be found at [SysPop Wiki](https://github.com/jzanetti/Syspop/wiki). Any questions please contact: _Sijin.Zhang@esr.cri.nz_**

### Contents:

* [Installation](https://github.com/jzanetti/Syspop#installation)
* [Usage](https://github.com/jzanetti/Syspop#usage)
* [Output](https://github.com/jzanetti/Syspop#output)
* [Release (for developer)](https://github.com/jzanetti/Syspop#release-for-developer)

## Installation
The package can be installed via:
```
pip install syspop
```

## Usage

### syspop.create 

A synthetic population can be created using:

```
from syspop.syspop import create as syspop_create

syspop_create(
    syn_areas = [135400, 111400, 110400],
    output_dir = "/tmp/syspop_test",
    pop_gender = pop_data["gender"],
    pop_ethnicity = pop_data["ethnicity"],
    geo_hierarchy = geog_data["hierarchy"],
    geo_location = geog_data["location"],
    geo_address = geog_data["address"],
    household = household_data["household"],
    socialeconomic = geog_data["socialeconomic"],
    work_data = work_data,
    home_to_work = commute_data["home_to_work"],
    school_data = school_data["school"],
    hospital_data = hospital_data["hospital"],
    supermarket_data = supermarket_data["supermarket"],
    restaurant_data = restaurant_data["restaurant"],
    assign_address_flag = True,
    rewrite_base_pop = False,
    use_parallel = True,
    ncpu = 8
)
```

Detailed descriptions of the input data for each argument can be found in [Input data](https://github.com/jzanetti/Syspop/wiki/Input-data).

It's important to note that all arguments in the ``syspop.create`` function are optional, and their requirement depends on the specific synthetic information that needs to be generated. To understand the interdependencies between different synthetic information, refer to the documentation available [here](https://github.com/jzanetti/Syspop/wiki/Synthetic-population)


### syspop.diary

The daily activity (diary) for the produced synthetic population can be created using the function ``syspop.diary``:

```
from syspop import diary as syspop_diary
syspop_diary(output_dir="/tmp/syspop", n_cpu=1)
```
where ``output_dir`` refers to the directory where the synthetic population is stored (e.g., ``/tmp/syspop/syspop_base.parquet``). The output will be stored in the directory as ``<output_dir>/diaries.parquet``. The details of creating diaries for synthetic population can be found [here](https://github.com/jzanetti/Syspop/wiki/Synthetic-diary)

### syspop.validate

The produced syntehtic population from ``syspop.create`` can be validated using the function of ``syspop.validate``:

```
from syspop.syspop import validate as syspop_validate
syspop_validate(
    output_dir=output_dir,
    pop_gender=pop_data["gender"],
    pop_ethnicity=pop_data["ethnicity"],
    household=household_data["household"],
    work_data=work_data,
    home_to_work=commute_data["home_to_work"],
)
```

The above codes provide validation for the produced synthetic population, which must be stored in ``<output_dir>/syspop_base.csv``. Details for synthetic population validation can be found [here](https://github.com/jzanetti/Syspop/wiki/Validation).

### syspop.vis

The produced syntehtic population from ``syspop.create`` can be plotted using the function of ``syspop.vis``:

```
from syspop.syspop import vis as syspop_vis
syspop_vis(
    output_dir=output_dir,
    plot_distribution=True,
    plot_travel=True,
    plot_location=True,
    travel_sample_size=250,
)
```
The above codes provide validation for the produced synthetic population, which must be stored in ``<output_dir>/syspop_base.csv``. Details for synthetic population visualization can be found [here](https://github.com/jzanetti/Syspop/wiki/Visualization).

## Output
The output from **Syspop** is a comprehensive table that contains the information for each synthetic individual, which can be used for any downstream analytic and modeling works. The list of attributes for each synthetic individual can be found [here](https://github.com/jzanetti/Syspop/wiki/Some-basic-attributes-for-Syspop)

|   area  | age | gender | ethnicity |   household    | ... | school |    primary_hospital    |     secondary_hospital      |             supermarket             |                     restaurant                     |
|---------|-----|--------|-----------|----------------|-----|--------|------------------------|-----------------------------|--------------------------------------|---------------------------------------------------|
| 236300  |  0  | female |  European | 236300_4_438   | ... |  NaN   | 237800_hospital_2_0   | 235800_hospital_28_0        | supermarket_284,supermarket_283    | restaurant_1407,restaurant_2551,restaurant_287... |
| 236300  |  13  | male   |  Maori    | 236300_8_189   | ... |  school_23134_primary   | 237800_hospital_2_0   | 235800_hospital_28_0        | supermarket_284,supermarket_283    | restaurant_1407,restaurant_2551,restaurant_287... |
| 236300  |  0  | female |  Maori    | 236300_4_638   | ... |  NaN   | 237800_hospital_2_0   | 235800_hospital_28_0        | supermarket_284,supermarket_283    | restaurant_1407,restaurant_2551,restaurant_287... |
| 236300  |  0  | male   |  Maori    | 236300_3_220   | ... |  NaN   | 237800_hospital_2_0   | 235800_hospital_28_0        | supermarket_284,supermarket_283    | restaurant_1407,restaurant_2551,restaurant_287... |
|   ...   | ... |  ...   |    ...    |      ...       | ... |  ...   |         ...            |             ...               |               ...                  |                      ...                          |

## Release (for developer)
The package can be released by running:

```
make publish
```

Note that before the release, it is suggested to check the latest version by ``make pkg_version``, and then adjust the version number in ``setup.py`` accordingly.