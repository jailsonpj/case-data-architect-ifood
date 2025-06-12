from pyspark.sql.types import *
from pyspark.sql import functions as F
from utils.utils import read_files_parquet, save_file_parquet
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransformedTLCData:
    """Transform TLC (Taxi & Limousine Commission) trip data.

    Performs the following operations:
    - Data type conversions
    - Column name standardization
    - Temporal column creation
    - Batch processing by month

    Attributes:
        s3_bucket (str): S3 bucket name for storage
        path_mount_raw (str): Base path for raw data
        path_mount_transformed (str): Base path for transformed data
    """

    def __init__(self):
        """Initialize class with default path settings."""
        self.s3_bucket = "transformed-tlc-trip-data-case-ifood"
        self.path_mount_raw = "/mnt/mount_raw/"
        self.path_mount_transformed = "/mnt/mount_transformed/"
        logger.info("Initialized TransformedTLCData with bucket: %s", self.s3_bucket)

    def convert_types_data(self, df):
        """Convert column data types according to predefined mapping.

        Args:
            df (pyspark.sql.DataFrame): Input DataFrame with raw data

        Returns:
            pyspark.sql.DataFrame: DataFrame with converted data types

        Note:
            Only converts columns that exist in the DataFrame
        """
        logger.debug("Starting data type conversion")
        type_conversions = {
            "VendorID": "integer",
            "tpep_pickup_datetime": "timestamp",
            "tpep_dropoff_datetime": "timestamp",
            "lpep_pickup_datetime": "timestamp",
            "lpep_dropoff_datetime": "timestamp",
            "passenger_count": "bigint",
            "trip_distance": "double",
            "RatecodeID": "integer",
            "PULocationID": "integer",
            "DOLocationID": "integer",
            "payment_type": "integer",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "improvement_surcharge": "double",
            "total_amount": "double",
            "congestion_surcharge": "double",
            "airport_fee": "double"
        }

        converted_columns = []
        for column, dtype in type_conversions.items():
            if column in df.columns:
                df = df.withColumn(column, F.col(column).cast(dtype))
                converted_columns.append((column, dtype))

        logger.info(
            "Converted %d columns: %s",
            len(converted_columns),
            [f"{col}->{typ}" for col, typ in converted_columns]
        )
        return df

    def standardize_column_names(self, df):
        """Standardize column names to snake_case format.

        Args:
            df (pyspark.sql.DataFrame): DataFrame with columns to standardize

        Returns:
            pyspark.sql.DataFrame: DataFrame with standardized column names

        Note:
            1. Applies specific mapping for known columns first
            2. For unmapped columns, applies generic transformations:
               - Lowercase
               - Replace spaces/hyphens with underscores
               - Remove parentheses
        """
        logger.debug("Starting column name standardization")
        column_mapping = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'tpep_pickup_datetime',
            'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
            'lpep_pickup_datetime': 'lpep_pickup_datetime',
            'lpep_dropoff_datetime': 'lpep_dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'PULocationID': 'pu_location_id',
            'DOLocationID': 'do_location_id',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount',
            'congestion_surcharge': 'congestion_surcharge',
            'airport_fee': 'airport_fee'
        }

        renamed_columns = []
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
                renamed_columns.append((old_name, new_name))

        generic_renames = []
        for col_name in df.columns:
            if col_name not in column_mapping.values():
                new_name = (
                    col_name.lower()
                    .replace(' ', '_')
                    .replace('-', '_')
                    .replace('(', '')
                    .replace(')', '')
                )
                if new_name != col_name:
                    df = df.withColumnRenamed(col_name, new_name)
                    generic_renames.append((col_name, new_name))

        logger.info(
            "Renamed %d specific and %d generic columns",
            len(renamed_columns),
            len(generic_renames)
        )
        logger.debug("Specific renames: %s", renamed_columns)
        logger.debug("Generic renames: %s", generic_renames)
        return df

    def create_columns_date(self, df, column):
        """Create temporal columns (year, month, day) from date column.

        Args:
            df (pyspark.sql.DataFrame): Input DataFrame
            column (str): Name of date/timestamp column

        Returns:
            pyspark.sql.DataFrame: DataFrame with new temporal columns
        """
        logger.debug("Creating date columns from %s", column)
        if column not in df.columns:
            logger.error("Column %s not found in DataFrame", column)
            raise ValueError(f"Column {column} not found")

        df = (
            df.withColumn("year", F.year(F.col(column)))
              .withColumn("month", F.month(F.col(column)))
              .withColumn("day", F.dayofmonth(F.col(column)))
        )
        logger.info("Created year/month/day columns from %s", column)
        return df

    def transformed_data(self, color="yellow", months=5):
        """Process taxi trip data by months.

        Args:
            color (str): Taxi fleet color (yellow/green)
            months (int): Number of months to process (1-12)

        Raises:
            ValueError: If invalid color or months value provided

        Note:
            - Processes 2023 data only
            - Saves data partitioned by year/month
            - Applies all pipeline transformations
        """
        logger.info(
            "Starting data transformation for %s taxi, %d months",
            color,
            months
        )

        if color not in ["yellow", "green"]:
            logger.error("Invalid color parameter: %s", color)
            raise ValueError("Color must be 'yellow' or 'green'")

        if not 1 <= months <= 12:
            logger.error("Invalid months parameter: %d", months)
            raise ValueError("Months must be between 1 and 12")

        column_date = "lpep_dropoff_datetime" if color == "green" else "tpep_dropoff_datetime"

        for month in range(1, months + 1):
            try:
                path = self.path_mount_raw + f"{color}_taxi/2023/{month}"
                logger.info("Processing month %d from path: %s", month, path)

                df = read_files_parquet(file_input=path)
                logger.info("Read %d records for month %d", df.count(), month)

                df = self.convert_types_data(df)
                df = self.standardize_column_names(df=df)
                df = self.create_columns_date(df=df, column=column_date)

                df = df.filter(F.col("year") == 2023)
                output_path = self.path_mount_transformed + f'{color}_taxi/2023/{month}'
                save_file_parquet(df, output_path)
                logger.info(
                    "Successfully saved transformed data to: %s",
                    output_path
                )

            except Exception as e:
                logger.error(
                    "Error processing month %d: %s",
                    month,
                    str(e),
                    exc_info=True
                )
                raise