# Data Engineering Test - Instructions:

To execute the test, follow the instructions below:
1 - Make sure you have Docker installed on your system.
2 - Open a terminal or command prompt.
3 - Navigate to the directory where the file is located.
4 - Build the Docker image by running the following command: docker-compose up
5 - To access the container's terminal, execute the following command:docker exec -it <nome_do_contêiner> bash (Replace <container_name> with the name of the container where the file you want to execute is located. You can check the names of the running containers using the docker ps command.)
6 - To generate the requested DataFrame in the test, execute the following command: spark-submit app/src/councils_job.py. The first 20 lines of the DataFrame will be display and the transform.csv file will be saved in the app/data/output directory.
7 - To run the unit tests, execute the following command: spark-submit /app/test/test_councils_job.py

-------------------------------------------------------------------------------------------------

# Data Engineering Test

Join multiple input files using PySpark to produce an output DataFrame in the required format.

## Input files
The input directory data (available in input_directory inside the EnglandCouncilsETLJob class) contains the following file structure:
```
data 
|-- england_councils
|   |-- district_councils.csv
|   |-- london_boroughs.csv
|   |-- metropolitan_districts.csv 
|   |-- unitary_authorities.csv
|-- property_avg_price.csv
|-- property_sales_volume.csv
```

### 1. An `england_councils` directory which contains four files:
  - `district_councils.csv`
  - `london_boroughs.csv`
  - `metropolitan_districts.csv`
  - `unitary_authorities.csv`

Each of these files contains data about a different type of local council.

They all have the same structure and include only two columns, as below:  
  
```council, county```

Here is a random row from one of the files:

| council  | county |
|----------|--------|
| Basildon | Essex  |

### 2. A `property_avg_price.csv` file.
 
This file provides data about average price by council, and contains the following four columns:

```
local_authority, avg_price_nov_2019, avg_price_nov_2018, difference
```

Here is a random row from this file:

| local_authority      | avg_price_nov_2019 | avg_price_nov_2018 | difference |
|----------------------|--------------------|--------------------|------------|  
| Barking and Dagenham | 317176.0           | 302477.0           | 4 90%      |

### 3. `property_sales_volume.csv` file.

This file provides data about sales volume by council and contains the following three columns:  

```
local_authority, sales_volume_sep_2019, sales_volume_sep_2018
```

Here is a random row from this file:

| local_authority      | sales_volume_sep_2019 | sales_volume_sep_2018 |
|----------------------|-----------------------|-----------------------|
| Amber Valley         | 156                   | 177                   |

## Requirements

Create a PvSpark DataFrame that contains data about each of the councils from the files in the `england_councils` director and enrich it by adding some additional columns from the `property_avg_price.csv` and `property_sales_volume.csv` files.  

To achieve this, please use PySpark and complete the following four methods on `src/councils_job.py`:

### 1. `extract_councils`
This method should use the files in the `england_councils` directory (available inside `input_directory`) as input. It should return a Spark DataFrame which combines the rows from all these files and contains following columns:
- `council` - this column contains the data from the council column from the raw files;
- `county` - this column contains the data from the county column from the raw files;
- `council_type` - this would be a new column based on the file from which the row is taken:
  - Rows from `district_councils.csv` should have the value District Council;
  - Rows from `london_boroughs.csv` should have the value London Borough;
  - Rows from `metropolitan_districts.csv` should have the value Metropolitan District;
  - Rows from `unitary_authorities.csv` should have the value Unitary Authority.

> **Hint:** This DataFrame should contain 316 rows, which is the sum of rows in the four files in the `england_councils` directory.

### 2. `extract_avg_price`
This method should use the file `property_avg_price.csv` (available inside `input_directory`) as input and return a DataFrame containing the following two columns:
- `council` - this column contains the data from the `local_authority` column from the raw files;
- `avg_price_nov_2019` - this column contains the data from the `avg_price_nov_2019` column from the raw files'.

### 3. `extract_sales_volume`
This method should use the file `property_sales_volume.csv` (available inside `input_directory`) as input and return a DataFrame containing the following two columns:
- `council` - this column contains the data from the `local_authority` column from the raw files;
- `sales_volume_sep_2019` - this column contains the data from the `sales_volume_sep_2019` column from the raw files.

### 4. `transform` 
This method uses the outbut DataFrames from the three previous methods. It should produce a DataFrame which joins them together and contains the following columns:
```
council, county, council_type, avg_price_nov_2019, sales_volume_sep_2019
```
  
Please join the data in such a wav that each council is still available in the outout even if it does not have any columns populated.  

Here are five example rows showing how the output DataFrame of the `transform` method should appear:

| council      | county         | council_type          | avg_price_nov_2019 | sales_volume_sep_2019 | 
|--------------|----------------|-----------------------|--------------------|-----------------------|
| Adur         | West Sussex    | District Council      | 316482.0           | 82                    |
| Durham       | Countv Durham  | Unitarv Authoritv     | null               | 608                   |  
| East Suffolk | Suffolk        | District Council      | null               | null                  |  
| Hacknev      | Greater London | London Borough        | 565018.0           | 128                   |  
| Knowsley     | Mersevside     | Metropolitan District | 133276.0           | 156                   |  
  
> **Hint:** This DataFrame should contain 316 rows, which is the same as the number of rows in the DataFrame returned by the `extract_councils` method.

## Hints
- Please modify only the four methods listed above and not the rest of the code.
- This task runs against Spark version 3.1.1. and Python 3.8.
- Feel free to implement unit tests on `tests` directory.
- Preferably, use docker so that we can run without having to follow an installation step by step.
- Add comments and docstrings using the pattern: https://google.github.io/styleguide/pyguide.html.
- Prefer to use type hints https://peps.python.org/pep-0484/
