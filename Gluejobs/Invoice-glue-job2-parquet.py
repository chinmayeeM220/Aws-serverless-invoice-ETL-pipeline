import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def main():
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger = glueContext.get_logger()
    
    logger.info("Starting Phase 2: Secure JSON to Parquet Transformation...")
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'json_src_bucket', 'parquet_dst_bucket'])
    json_src = args['json_src_bucket']
    parquet_dst = args['parquet_dst_bucket']
    
    # 1. EXPLICITLY DEFINE SCHEMA TO PREVENT _CORRUPT_RECORD ERRORS
    defined_schema = StructType([
        StructField("source_metadata", StructType([
            StructField("input_file_name", StringType(), True),
            StructField("origin_bucket", StringType(), True),
            StructField("status", StringType(), True)
        ]), True),
        StructField("extracted_invoice_data", StructType([
            StructField("vendor_name", StringType(), True),
            StructField("invoice_number", StringType(), True),
            StructField("invoice_date_raw", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("grand_total", DoubleType(), True)
        ]), True)
    ])
    
    try:
        source_path = f"s3://{json_src}/*.json"
        logger.info(f"Reading data defensively from: {source_path}")
        
        # Pass the schema explicitly and enable multiline JSON parsing 
        raw_df = spark.read.schema(defined_schema).option("multiLine", "true").json(source_path)
        
        # 2. SAFE COUNT GUARDRAIL (Bypasses .rdd structure compilation error)
        if raw_df.count() == 0:
            logger.info("Target bucket contains no valid records. Exiting gracefully.")
            return

        # 3. TRANSFORM
        flattened_df = raw_df.select(
            col("source_metadata.input_file_name").alias("file_name"),
            col("extracted_invoice_data.vendor_name").alias("vendor_name"),
            col("extracted_invoice_data.invoice_number").alias("invoice_number"),
            col("extracted_invoice_data.invoice_date_raw").alias("invoice_date"),
            col("extracted_invoice_data.currency").alias("currency"),
            col("extracted_invoice_data.grand_total").alias("grand_total")
        )
        
        # Drop any records that couldn't parse minimal critical data fields
        clean_df = flattened_df.filter(col("vendor_name").isNotNull())
        
        # 4. LOAD
        destination_path = f"s3://{parquet_dst}/warehouse/invoices/"
        logger.info(f"Writing Parquet file format to: {destination_path}")
        
        clean_df.write \
            .mode("overwrite") \
            .partitionBy("vendor_name") \
            .parquet(destination_path)
            
        logger.info("Parquet transformation finished cleanly.")
        
    except Exception as error:
        logger.error(f"FATAL ERROR RUNNING TRANSFORMATION: {str(error)}")
        raise error
    finally:
        sc.stop()

if __name__ == '__main__':
    main()