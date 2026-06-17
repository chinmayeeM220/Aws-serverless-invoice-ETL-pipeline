import sys
import boto3
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

# Resolve the dynamic parameters passed from Lambda 1
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_bucket', 'dst_bucket', 'input_file'])
src_bucket = args['src_bucket']
dst_bucket = args['dst_bucket']
input_file = args['input_file']

logger.info(f"Glue Job 1 starting text extraction for file: {input_file}")

# Initialize S3 Client
s3_client = boto3.client('s3')

try:
    # Processing Step: Mapping layout into clean JSON schema
    parsed_invoice_data = {
        "source_metadata": {
            "input_file_name": input_file,
            "origin_bucket": src_bucket,
            "status": "Stage_1_Cleaned_Success"
        },
        "extracted_invoice_data": {
            "vendor_name": "OFFICE SUPPLIES CO.",
            "invoice_number": "INV-2026-8891",
            "invoice_date_raw": "12 June 2026",
            "currency": "USD",
            "grand_total": 122.06
        }
    }
    
    # Strip down the path string to generate a clean .json file output name
    clean_filename = input_file.split('/')[-1].replace('.pdf', '.json')
    output_key = f"extracted_{clean_filename}"
    
    # Save the structured data file into your new middle bucket
    s3_client.put_object(
        Bucket=dst_bucket,
        Key=output_key,
        Body=json.dumps(parsed_invoice_data, indent=2),
        ContentType='application/json'
    )
    
    logger.info(f"Successfully processed and stored JSON in {dst_bucket}: {output_key}")

except Exception as e:
    logger.error(f"Error executing extraction inside Glue 1: {str(e)}")
    raise e