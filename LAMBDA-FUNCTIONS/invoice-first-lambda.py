import boto3
import json

# Initialize the AWS Glue client
client = boto3.client('glue')

def lambda_handler(event, context):
    print("Lambda 1 function has been triggered via SQS queue::::")
    print(json.dumps(event))
    
    for i in event['Records']:
        # SQS wraps the original S3 bucket payload message inside the 'body' key string
        s3_event = json.loads(i['body'])
        
        if 'Event' in s3_event and s3_event['Event'] == 's3:TestEvent':
            print("S3 Test Event Connection Detected.")
        else:
            for j in s3_event['Records']:
                # Dynamically isolate tracking keys from the S3 PUT message
                sourcebucket = j['s3']['bucket']['name']
                sourcefilename = j['s3']['object']['key']
                
                input_file = sourcefilename
                print("The input invoice file received is:", input_file)
                
                # Start Glue Job 1 and pass structural execution arguments
                response = client.start_job_run(
                    JobName='Invoice_Glue_Job_1',
                    Arguments={
                        '--src_bucket': sourcebucket,
                        '--dst_bucket': 'invoice-pipe-output-bucket',
                        '--input_file': input_file
                    }
                )
                return response