import sys
sys.path.append('/app/')
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from src.councils_job import CouncilsJob


class CouncilsJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("CouncilsJobTest").getOrCreate()

    def test_extract_councils(self):
        job = CouncilsJob()
        councils_df = job.extract_councils()

        print("Councils DataFrame:")
        councils_df.show(10)

        self.assertEqual(len(councils_df.columns), 3)
        self.assertTrue("council" in councils_df.columns)
        print("The 'council' column is present")
        self.assertTrue("county" in councils_df.columns)
        print("The 'county' column is present")
        self.assertTrue("council_type" in councils_df.columns)
        print("The 'council_type' column is present")
        self.assertEqual(councils_df.filter(councils_df.council_type == "District Council").count(), 192)
        self.assertEqual(councils_df.filter(councils_df.council_type == "London Borough").count(), 33)
        self.assertEqual(councils_df.filter(councils_df.council_type == "Metropolitan District").count(), 36)
        self.assertEqual(councils_df.filter(councils_df.council_type == "Unitary Authority").count(), 55)
        self.assertEqual(councils_df.count(), 316)

    def test_extract_avg_price(self):
        job = CouncilsJob()
        avg_price_df = job.extract_avg_price()

        print("Avg Price DataFrame:")
        avg_price_df.show(10)

        self.assertEqual(len(avg_price_df.columns), 2)
        self.assertTrue("council" in avg_price_df.columns)
        print("The 'council' column is present")
        self.assertTrue("avg_price_nov_2019" in avg_price_df.columns)
        print("The 'avg_price_nov_2019' column is present")

    def test_extract_sales_volume(self):
        job = CouncilsJob()
        sales_volume_df = job.extract_sales_volume()

        print("Sales Volume DataFrame:")
        sales_volume_df.show(10)

        self.assertEqual(len(sales_volume_df.columns), 2)
        self.assertTrue("council" in sales_volume_df.columns)
        print("The 'council' column is present")
        self.assertTrue("sales_volume_sep_2019" in sales_volume_df.columns)
        print("The 'sales_volume_sep_2019' column is present")

    def test_transform(self):
        job = CouncilsJob()
        councils_df = job.extract_councils()
        avg_price_df = job.extract_avg_price()
        sales_volume_df = job.extract_sales_volume()
        transformed_df = job.transform(councils_df, avg_price_df, sales_volume_df)

        print("Transformed DataFrame:")
        transformed_df.show(10)

        self.assertEqual(transformed_df.count(), 316)


if __name__ == "__main__":
    unittest.main()