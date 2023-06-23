import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

class CouncilsJob:
    
    def __init__(self):
        self.spark_session = SparkSession.builder.appName("EnglandCouncilsJob").getOrCreate()
        self.input_directory = os.getenv("DATA_DIR")

    def extract_councils(self):
        """
        Extracts councils data from CSV files and returns a combined DataFrame. 
        Use the files in the `england_councils` directory (available inside `input_directory`).

        Returns:
            DataFrame which combines the rows from all these files and contains following columns:
            - `council` - this column contains the data from the council column from the raw files;
            - `county` - this column contains the data from the county column from the raw files;
            - `council_type` - this would be a new column based on the file from which the row is taken:
            - Rows from `district_councils.csv` should have the value District Council;
            - Rows from `london_boroughs.csv` should have the value London Borough;
            - Rows from `metropolitan_districts.csv` should have the value Metropolitan District;
            - Rows from `unitary_authorities.csv` should have the value Unitary Authority.
        """        
        files = ['district_councils.csv', 'london_boroughs.csv', 'metropolitan_districts.csv', 'unitary_authorities.csv']
        council_types = ['District Council', 'London Borough', 'Metropolitan District', 'Unitary Authority']
        dfs = []
        for file, council_type in zip(files, council_types):
            file_path = os.path.join(self.input_directory, 'england_councils', file)
            df = self.spark_session.read.option("header", True).csv(file_path)
            df = df.withColumn('council_type', lit(council_type))
            dfs.append(df)
        councils_df = dfs[0]
        for i in range(1, len(dfs)):
            councils_df = councils_df.union(dfs[i])
        return councils_df

    def extract_avg_price(self):
        """
        Extracts the average price data from the 'property_avg_price.csv' file.

        Returns:
             DataFrame containing the following two columns:
            - `council` - this column contains the data from the `local_authority` column from the raw files;
            - `avg_price_nov_2019` - this column contains the data from the `avg_price_nov_2019` column from the raw files'.
        """        
        file_path = os.path.join(self.input_directory, 'property_avg_price.csv')
        avg_price_df = (self.spark_session.read.option("header", True).csv(file_path)
                    .select('local_authority', 'avg_price_nov_2019')
                    .withColumnRenamed('local_authority', 'council'))
        return avg_price_df

    def extract_sales_volume(self):
        """
        Extracts the sales volume data from the 'property_sales_volume.csv' file.

        Returns:
           DataFrame containing the following two columns:
            - `council` - this column contains the data from the `local_authority` column from the raw files;
            - `sales_volume_sep_2019` - this column contains the data from the `sales_volume_sep_2019` column from the raw files.
        """
        file_path = os.path.join(self.input_directory, 'property_sales_volume.csv')

        # Read the CSV file and select the required columns, rename 'local_authority' to 'council'
        sales_volume_df = (self.spark_session.read.option("header", True).csv(file_path)
                           .select('local_authority', 'sales_volume_sep_2019')
                           .withColumnRenamed('local_authority', 'council'))
        return sales_volume_df

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        """
        Transform the input DataFrames by performing a left outer join and selecting specific columns.
     
        Returns: DataFrame which joins them together and contains the following columns: 
                 - council, county, council_type, avg_price_nov_2019, sales_volume_sep_2019 
        """
        transform_df = councils_df.join(avg_price_df, "council", "left_outer") \
                               .join(sales_volume_df, "council", "left_outer") \
                               .select(councils_df.council, councils_df.county, councils_df.council_type,
                                       avg_price_df.avg_price_nov_2019, sales_volume_df.sales_volume_sep_2019)

        # Sort the DataFrame in ascending order based on the first column
        transform_df = transform_df.orderBy(transform_df.columns[0])

        return transform_df

    def run(self):
        """
        Runs the England Councils ETL job.
    
        Returns: The DataFrame 'transform' containing the joined data.
        """
        # Extract data
        councils_df = self.extract_councils()
        avg_price_df = self.extract_avg_price()
        sales_volume_df = self.extract_sales_volume()
    
        # Transform the data by joining the DataFrames
        transform_df = self.transform(councils_df, avg_price_df, sales_volume_df)
        # Define the output path
        output_path = os.path.join(self.input_directory, 'output', 'transform.csv')
        # Write the transformed DataFrame to the output path as CSV
        transform_df.coalesce(1).write.mode("append").format("csv").option("header", True).option("sep", ",").option("quoteAll", True).save(output_path)

        return transform_df
        
job = CouncilsJob()
result_df = job.run()
result_df.show()
