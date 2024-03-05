import findspark
findspark.init()
import logging
import os
from os import listdir
from os.path import isfile, join
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CsvPipeline:
    def __init__(self, spark_session: SparkSession, filepath: str, options: dict = None):
        """
        Initializes the CSV pipeline with a Spark session, file path, and optional read options.
        """
        self.spark = spark_session
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self.filepath = filepath
        self.options = options if options is not None else {}

    def show_schema(self):
        """
        Reads the CSV file and prints its schema.
        """
        #df = self.read_csv_pipeline()
        self.df.printSchema()

    def show_head(self, n: int = 5):
        """
        Reads the CSV file and shows the first n rows.
        """
        #df = self.read_csv_pipeline()
        df.show(n)

    def get_files(self) -> list:
        """
        Returns a list of CSV file paths in the specified directory.
        """
        # Initialize an empty list to store the full file paths
        csv_files = []
        
        # Check if the filepath exists and is a directory
        if not os.path.exists(self.filepath):
            logger.error(f"Directory does not exist: {self.filepath}")
            return csv_files  # Return empty list if directory does not exist

        if not os.path.isdir(self.filepath):
            logger.error(f"Path is not a directory: {self.filepath}")
            return csv_files  # Return empty list if filepath is not a directory

        # Iterate over the files in the specified directory
        for file in os.listdir(self.filepath):
            # Check if the file is a CSV file (case-insensitive)
            if file.lower().endswith('.csv'):
                full_path = os.path.join(self.filepath, file)  # Construct the full file path
                csv_files.append(full_path)  # Add the full file path to the list

        print("CSV FIles names: ", csv_files)
        # Return the list of CSV file paths
        return csv_files


    def expected_columns_for_file(self, filename: str) -> list:
        """
        Returns the expected columns based on the file name.
        """
        expected_columns = {
            'customers': ['customer_id', 'customer_name', 'customer_lastname', 'country_id'],
            'countries': ['country_id', 'country_description'],
            'orders': ['order_id', 'order_date', 'order_amount', 'order_customer_id']
        }
        return expected_columns.get(filename, None)

    def validate_and_process_files(self):
        """
        Validates and processes each CSV file in the directory.
        """
        files = self.get_files()
        for filepath in files:
            filename = os.path.basename(filepath).replace('.csv', '')  # Adjusted to remove '.csv' for comparison
            expected_columns = self.expected_columns_for_file(filename)
            if not expected_columns:
                logger.warning(f"No expected columns defined for {filename}")
                continue
            self.process_file(filepath, expected_columns)

    def process_file(self, filepath: str, expected_columns: list):
        """
        Processes a single CSV file.
        """
        # Check if the file exists
        if not os.path.exists(filepath):
            logger.error(f"File does not exist: {filepath}")
            return

        # Check if the file is readable (i.e., has read permissions)
        if not os.access(filepath, os.R_OK):
            logger.error(f"File is not accessible (read permissions required): {filepath}")
            return

        # If the file exists and is readable, proceed to read it into a DataFrame
        try:
            print("File paaaath:", filepath)
            df = self.spark.read.format("csv").options(**self.options).load(filepath)
            print("Reading completed at process file")
        except Exception as e:
            logger.error(f"Failed to read file {filepath}: {str(e)}")
            return

        # Check for missing expected columns
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing expected columns: {missing_columns} in file {filepath}")
            return

        # Assuming additional processing steps would be defined here
        logger.info(f"Successfully processed {filepath}")

        #self.remove_duplicates(df)
        #self.handle_missing_values(df, 'remove')

        self.write_to_delta(df, os.path.splitext(os.path.basename(filepath))[0])


    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Removes duplicate rows from a DataFrame.
        """
        try:
            deduplicated_df = df.dropDuplicates()
            logger.info("Duplicate rows removed")
            return deduplicated_df
        except Exception as e:
            logger.error(f"Error removing duplicate rows: {str(e)}")
            raise

    def handle_missing_values(self, df: DataFrame, action: str = 'remove') -> DataFrame:
        """
        Handles missing values in the DataFrame based on the specified action ('remove' or 'report').
        """
        try:
            if action == 'remove':
                cleaned_df = df.na.drop()
                logger.info("Rows with missing values removed")
            elif action == 'report':
                for column in df.columns:
                    missing_count = df.filter(col(column).isNull()).count()
                    if missing_count > 0:
                        logger.info(f"Column {column} has {missing_count} missing values")
                cleaned_df = df  # Return original DataFrame if action is 'report'
            else:
                logger.error("Invalid action for handling missing values")
                raise ValueError("Invalid action for handling missing values")
            return cleaned_df
        except Exception as e:
            logger.error(f"Error handling missing values: {str(e)}")
            raise
    
    def write_to_delta(self, df, filename):
        df.show()
        #print("Value variable filename: ", filename)
        # Define the path for the Delta table, incorporating the filename
        delta_table_path = os.path.join("//Users/augustobarbosa/Py_Projects/CSV-Pipeline/csv-pipeline/docs/", filename)
        print("Value variable delta_table_path: ", delta_table_path)
        # Write the DataFrame to Delta Lake, creating a separate table for each file
        pathname2 = delta_table_path+filename
        df.write.format("delta").save("/tmp/teste")
        print("Saving successfull")


    def read_csv_pipeline(self):
        """
        Reads a CSV file into a Spark DataFrame with the provided options.
        """
        # Validate and process files
        self.validate_and_process_files()
        