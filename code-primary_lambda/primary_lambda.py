import json
import uuid
import boto3
import math
import os

# Set ENV variables
from dotenv import load_dotenv
load_dotenv()



# This is the part your need to change
os.environ["BUCKET_NAME"]= 'temporary-evaluation-files'
os.environ["FOLDER_NAME"] = 'files'

os.environ["TASK_DEFINITION"] = 'container-model-path:36'
os.environ["CONTAINER_NAME"] = 'evalcontainer'

os.environ["SUBNET"] = 'subnet-0aa9afa04b5cdb9f1' # Can set multiple: SUBNET_1, SUBNET_2..etc
os.environ["SECURITY_GROUP"] = 'sg-083f21a63b65480b3' # Can set multiple: SECURITY_GROUP_1...etc



def lambda_handler(event, context):

    """
    AWS Lambda function to handle the event, format the data, upload to S3, and execute ECS tasks.
    
    Args:
        event (dict): The event data that triggered the Lambda function. Expected to contain the 
                      JSON body with 'file_batch', 'metrics', and 'task_quantity'.
        context (object): The runtime information of the Lambda function.

    Returns:
        dict: A dictionary containing the status code, body with the task details (task ARNs, 
              result versions, failed tasks count), and headers.

    Key Components:

        formatfile: Transforms JSON payload content into an ENV format - metrics, version_id, file content
        S3upload: Uploads ENV file to S3
        Runtask: Executes the task using the ENV file as input variables
        
    """

    ######## SUPPORT FUNCTIONS #########

    # Formats the JSON event content into a suitable ENV file for the ECS Task
    def formatfile(file_content_json, metrics):
        
        # Convert to raw JSON
        file_content = json.dumps(file_content_json)
        
        # Generate a UUID for version indentification
        result_version = str(uuid.uuid4())
        
        # Split the JSON string into two parts as env variables for Fargate has a byte limit
        max_bytes=62000 # Actual AWS Byte limit is 65536, but give buffer in case of difference in encoding

        file_content_parts=[]

        if len(file_content) > max_bytes:
            num_files = math.ceil(len(file_content)/max_bytes)
            for i in range(num_files):
                start = i * max_bytes
                end = min((i + 1) * max_bytes, len(file_content))
                file_content_part = file_content[start:end]
                file_content_parts.append(file_content_part)
        else:
            file_content_parts.append(file_content)
            
        # Create the formatted string in ENV File format
        env_file_content = f"METRICS={metrics}\nRESULT_VERSION={result_version}"
        for i, part in enumerate(file_content_parts):
            env_file_content += f"\nFILE_CONTENT_{i+1}={part}"
        
        return env_file_content, result_version

    # Upload the ENV File to S3 bucket
    def s3upload(s3_client, BUCKET_NAME, file_content):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f'{FOLDER_NAME}/file-{result_version}.env',
            Body=file_content,
            ContentType='application/json'
        )
        print(f'Successfully uploaded file to {BUCKET_NAME}/{FOLDER_NAME}/file-{result_version}.env')

    # Executes the ECS Task, USING the user specifice Subnet and SG
    def runtask(ecs_client, result_version):
    # Run ECS Task
        response = ecs_client.run_task(
            cluster='evaluationcluster',
            taskDefinition=TASK_DEFINTION,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [SUBNET],
                    'securityGroups': [SECURITY_GROUP],
                    'assignPublicIp': 'ENABLED' # Required to connect to S3
                }
            },
            # Overrides the task definition for the container, this is to specify the unique ENV file with the 
            # version ID generated above
            overrides={
                'containerOverrides': [
                    {
                        'name': CONTAINER_NAME, # The container name here must be the same name as in the task definition 
                        'environmentFiles': [
                            {
                                'value': f'arn:aws:s3:::{BUCKET_NAME}/{FOLDER_NAME}/file-{result_version}.env',
                                'type': 's3'
                            },
                        ],
                    }
                ]
            }
        )
        
        # Check if task started and return the task ARNS and number of failed tasks
        if not response['tasks']:
            print('Failed to start task')
            failed_tasks = int(os.environ.get('FAILED_TASKS')) + 1
            os.environ['FAILED_TASKS'] = str(failed_tasks)
            return 
        
        task_arn = response['tasks'][0]['taskArn']
        
        print('Task started successfully')

        return task_arn


    ######## MAIN CODY BODY #########

    #Extract Event
    body = event.get('body', '{}')
    data = json.loads(body)
    file_batch_json = data.get('file_batch', [])
    metrics = data.get('metrics', [])
    task_quantity = data.get('task_quantity', 1)

    # Create AWS Service Clients
    s3_client = boto3.client('s3')
    ecs_client = boto3.client('ecs')

    # Setting ENV variabeles
    BUCKET_NAME = os.environ.get("BUCKET_NAME")
    FOLDER_NAME = os.environ.get("FOLDER_NAME")

    TASK_DEFINTION = os.environ.get("TASK_DEFINITION")
    CONTAINER_NAME = os.environ.get("CONTAINER_NAME")

    SUBNET = os.environ.get("SUBNET")
    SECURITY_GROUP = os.environ.get("SECURITY_GROUP")

    
    try:
        os.environ['FAILED_TASKS'] = '0'
        task_arns = [] # A list to accomodate for multiple tasks in ECS
        result_versions=[] # A list to accomodate for multiple files when multiple tasks are used in ECS
        payload = {} # JSON event payload

        # Iterate through the number of tasks and execute a unique task for each
        for i in range(task_quantity):

            # Format the input data and generate a UUID
            file_batch, result_version = formatfile(file_batch_json[i], metrics)

            # Upload the formatted string to S3
            s3upload(s3_client, BUCKET_NAME, file_batch)
                
             # Run an ECS task with the uploaded file
            task_arn = runtask(ecs_client, result_version)
                
            # Store the task ARN and version ID
            task_arns.append(task_arn)

            result_versions.append(result_version)
            
            # If the number of failed tasks = number of tasks, that means all tasks failed, return error
            if int(os.environ.get('FAILED_TASKS')) == task_quantity:
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'Failed to start all tasks'})
                }
        
        # Load the JSON payload
        payload['failed_tasks'] = int(os.environ.get('FAILED_TASKS'))
        payload['task_arns'] = task_arns
        payload['result_versions'] = result_versions
        payload['task_quantity']= task_quantity
        
        # Return the content as JSON
        return {
            'statusCode': 200,
            'body': json.dumps(payload),
            'headers': {
                'Content-Type': 'application/json'
            }}
        
    # Exception Handling
    except Exception as e:
        print(f'Error: {e}')
        return {
            'statusCode': 500,
            'body': f'Internal Server Error: {e}'
        }
    

