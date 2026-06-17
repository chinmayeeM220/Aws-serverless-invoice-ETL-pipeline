import boto3
import json

def lambda_handler(event, context):
    # Initialize the AWS Glue client
    glue = boto3.client("glue")
    
    # Define your configuration parameters explicitly for Phase 2
    job_name = "Invoice_Glue_Job_2_Parquet"
    json_source_bucket = "invoice-pipe-output-bucket"
    parquet_destination_bucket = "invoice-analytics-parquet-bucket"
    
    print(f"Initiating execution for data transformation stage: {job_name}")
    
    try:
        # Trigger the Parquet transformation job with required static arguments
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--json_src_bucket': json_source_bucket,
                '--parquet_dst_bucket': parquet_destination_bucket
            }
        )
        
        job_run_id = response["JobRunId"]
        print(f"Glue job successfully invoked. Allocation Execution ID: {job_run_id}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Successfully started Glue transformation pipeline: {job_name}",
                "jobRunId": job_run_id
            })
        }
        
    except Exception as e:
        print(f"CRITICAL ERROR invoking Glue Job 2: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Failed to trigger transformation pipeline",
                "details": str(e)
            })
        }
