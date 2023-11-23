from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import os
import pyspark.sql.functions as F



class DataTransformer:
    """
    A class to perform transformations on PySpark DataFrames.

    Attributes:
    - spark: SparkSession instance created for performing Spark operations.
    """

    def __init__(self):
        """
        Initializes the DataTransformer class by creating a SparkSession instance.

        Args:
        - None

        Returns:
        - None
        """
        os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-21"
        self.spark = SparkSession.builder.appName("global_aptitude").getOrCreate()


    def cast_column(self, df: DataFrame, col: str, coltype: str) -> DataFrame:
        """
        Casts a column in a PySpark DataFrame to a specified type.

        Args:
        - df: PySpark DataFrame
        - col: Name of the column to be cast
        - coltype: Target data type to which the column will be cast (e.g., 'integer', 'string')

        Returns:
        - DataFrame: DataFrame with the specified column cast to the desired type
        """
        return df.withColumn(col, df[col].cast(coltype))



    def enforce_schema(self, df: DataFrame, schema: StructType, verbose: bool = False) -> DataFrame:
        """
        Enforces a specified schema on a PySpark DataFrame by adjusting column types
        and ensuring columns match the desired schema.

        Args:
        - df: PySpark DataFrame
        - schema: Desired schema (pyspark.sql.types.StructType)

        Returns:
        - DataFrame: DataFrame with enforced schema
        """
        df = self.rename_columns_to_lowercase(df)
        
        for col_name in schema.fieldNames():
            col_name_lower = col_name.lower()
            
            if col_name_lower in df.columns:
                current_type = df.schema[col_name_lower].dataType
                desired_type = schema[col_name].dataType
                
                if current_type != desired_type:
                    if verbose:
                        print(f"Column '{col_name_lower}' has type '{current_type}' in DataFrame, but type '{desired_type}' in schema.")
                    df = df.withColumn(col_name_lower, F.col(col_name_lower).cast(desired_type))
            else:
                print(f"Column '{col_name_lower}' does not exist in DataFrame.")
                
        return df


    def rename_columns_to_lowercase(self, df: DataFrame) -> DataFrame:
        """
        Renames columns in a PySpark DataFrame to lowercase.

        Args:
        - df: PySpark DataFrame

        Returns:
        - DataFrame: DataFrame with columns renamed to lowercase
        """
        new_columns = [F.col(column).alias(column.lower()) for column in df.columns]
        return df.select(*new_columns)

    
    def load_data(self, path_parquet: str) -> DataFrame:
        """
        Loads data from a Parquet file into a PySpark DataFrame.

        Args:
        - path_parquet: Path to the Parquet file

        Returns:
        - DataFrame: DataFrame containing the data loaded from the Parquet file
        """
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.option("header", "true").parquet(path_parquet)
        return df
    

    def drop_null_columns(self, df: DataFrame, threshold_percentage: int = 25) -> DataFrame:
        """
        Drops columns in a PySpark DataFrame that exceed a specified threshold of null values.
        
        Args:
        - df: PySpark DataFrame
        - threshold_percentage: Threshold percentage of null values to determine column removal (default: 25)
        
        Returns:
        - DataFrame: Modified DataFrame with columns removed that exceed the threshold of null values
        """
        print(f"Deleting columns that exceed {threshold_percentage}% of null values")
        total_rows = df.count()
        null_counts = [F.col(c).isNull().cast("int").alias(c) for c in df.columns]
        null_counts_df = df.select(null_counts)
        
        columns_to_drop = []
        for col_name in df.columns:
            null_count = null_counts_df.agg({f"{col_name}": "sum"}).collect()[0][0]
            null_percentage = (null_count / total_rows) * 100 if total_rows != 0 else 0
            
            if null_percentage > threshold_percentage:
                print(f"Column {col_name} marked to remove, {null_percentage}% of null values found")
                columns_to_drop.append(col_name)
        
        return df.drop(*columns_to_drop)


    # Analysis
    def show_nulls_per_column(self, df: DataFrame) -> None:
        """
        Displays the count and percentage of null values per column in a PySpark DataFrame.
        
        Args:
        - df: PySpark DataFrame
        
        Returns:
        - None: Prints the count and percentage of null values for each column in the DataFrame.
        """
        num_rows = df.count()
        null_counts = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
        null_counts_df = df.agg(*null_counts)
        null_counts_dict = null_counts_df.collect()[0].asDict()
        
        for col_name, null_count in null_counts_dict.items():
            print(f"{col_name}: {null_count} | {round(null_count / num_rows, 5) * 100}%")



    def create_sum_col(self, df: DataFrame, list_cols_to_sum: list) -> DataFrame:
        """
        Adds a new column that represents the sum of selected columns in a PySpark DataFrame.

        Args:
        - df: PySpark DataFrame
        - list_cols_to_sum: List of column names to be summed

        Returns:
        - DataFrame: Modified DataFrame with an added column 'total_amount_computed' 
          containing the sum of selected columns.
        """
        total_amount_col = sum(F.col(c) for c in list_cols_to_sum if c in df.columns)
        return df.withColumn("total_amount_computed", total_amount_col)
    

    def replace_column(self, df: DataFrame, col_to_replace: str, replace_with: str) -> DataFrame:
        """
        Replaces a column in a PySpark DataFrame by deleting col_to_replace and renaming replace_with to col_to_replace.
        
        Args:
        - df: PySpark DataFrame
        - col_to_replace: Name of the column to be replaced
        - replace_with: Name of the column that replaces the original column
        
        Returns:
        - Modified PySpark DataFrame with the column replaced
        """
        if not isinstance(df, DataFrame):
            raise ValueError("Input 'df' must be a PySpark DataFrame")
        
        if col_to_replace not in df.columns:
            raise ValueError(f"Column '{col_to_replace}' does not exist in the DataFrame")
        
        if replace_with not in df.columns:
            raise ValueError(f"Column '{replace_with}' does not exist in the DataFrame")
        
        # Dropping the original column
        df = df.drop(col_to_replace)
        
        # Renaming the replace_with column to col_to_replace
        df = df.withColumnRenamed(replace_with, col_to_replace)
        
        return df


    def get_spark_session(self):
        return self.spark