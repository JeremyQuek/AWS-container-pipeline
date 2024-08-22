import requests
import os
import sys
import time
import io, pandas as pd
import base64
import math

from dotenv import load_dotenv
load_dotenv()


def evaluateModel(file, metrics="ALL", refreshRate=10, refreshMessage=False, max_batch_size=100000):
    
    """
    Evaluates with an Excel file as an input:

    Args:
        file (str): Path to the Excel file for evaluation
        metrics (list, optional): List of metrics to evaluate. Defaults to "ALL".
        refreshRate (int, optional): Refresh rate for status checks on results completion. Defaults to 10s.
        refreshMessage (bool, optional): Whether to display refresh messages. Defaults to False.
        aws_api_key (str, optional): AWS API key for authentication. Defaults to None.
        max_batch_size (int, optional): Maximum batch size for for up-scaling tasks. Defaults to an arbitrary large number to use a single task only.

    Returns:
        dict: Final averaged evaluation scores for each metric across all questions
        df: Individual evaluation scores for each metric, for each question

    Key Components:

        encode_excel: Transforms the user's file to a base64 string, split according to the number of tasks
        api_call_primary: Calls on the primary Lambda function to execute tasks
        estimate_buffer_time: Sleeps for the time estimated for coldstart, preventing api calls to second Lambda function
        api_call_secondary: Implements a refresh-rate loop for calling the second Lambda function after the cold-start buffer
        prints the results and save to HTML/Excel
            
    Usage: 
    results = evaluateModel(file=file,
              metrics=metrics,
              refreshMessage=True
              )

    Running this will automatically print the results in the user's terminal window

    If the user wishes to see the results in the terminal window again, they can do: print(results)
    """


    ######## SUPPORT FUNCTIONS #########

    # This function converts the base64 string to an excel sheet
    def encode_excel_to_base64(file_path):
        # Read the Excel file
        df = pd.read_excel(file_path)
        batch_size = len(df)

        # TO divide the file evenly based on the number of tasks
        num_batches = math.ceil(len(df) / max_batch_size)
        file_list = [df[i * max_batch_size:(i + 1) * max_batch_size] for i in range(num_batches)]
        file_batch = []
        for i in range(num_batches):
            # Create a BytesIO object
            excel_buffer = io.BytesIO()
            
            # Write the DataFrame to the BytesIO object
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                file_list[i].to_excel(writer, index=False)
            
            # Get the bytes from the BytesIO object
            excel_bytes = excel_buffer.getvalue()
            
            # Encode the bytes to base64
            base64_encoded = base64.b64encode(excel_bytes).decode('utf-8')
            file_batch.append(base64_encoded)
            
        return file_batch, batch_size

    # This function calls on the primary lambda's api and handles any erros
    def api_call_primary(file_batch, metrics, headers, batch_size):

        # Prep API Call
        payload = {}
        payload['file_batch'] = file_batch
        payload['task_quantity'] = math.ceil(batch_size / max_batch_size)

        # Print out any metrics
        payload['metrics'] = metrics

        # API URL
        primary_lambda = PRI_LAMBDA_API
        
        # Await Response
        primary_response = requests.post(primary_lambda, json = payload, headers=headers)

        # Handle response
        if primary_response.status_code == 200:
            
            print(f"Nodes: Succesfully created, beginning process now")

            if payload['task_quantity'] >1:
                print(f"Running: {payload['task_quantity']} concurrent tasks. This may take a few minutes")

        else: 
            print(f"Error encountered: {primary_response.text}")
            sys.exit(1)

        return primary_response

    # This function periodically calls on the second API based on the user specificed refreshRate. 
    # The calling begins after the cold-start buffer time
    def api_call_secondary():

        # Get Json from secondary lambda
        while True:

            secondary_lambda = SEC_LAMBDA_API

            # Send only the body content to the second URL
            secondary_response = requests.post(secondary_lambda, json=primary_response_body , headers=headers)

            if secondary_response.status_code == 200:
                # Task completed successfully
                return secondary_response
            
            elif secondary_response.status_code == 400:
                #Retry
                if refreshMessage==True:
                    print(f"Evaluation job still running. Next status check in {refreshRate}s...") 

                time.sleep(refreshRate)

            elif secondary_response.status_code == 500:
                # Error occurred
                print(f"Error: {secondary_response.text}")
                sys.exit(1)
            else:
                print(f"Unexpected status code: {secondary_response.status_code}.{secondary_response.text}")
                sys.exit(1)

    # To cater for the initial sleep time for the cold start, prevents constant pinging while the task is starting up
    def estimate_buffer_time(batch_size, max_batch_size):

        # Rough time per lynx inference
        if "Lynx" in metrics:
            lynx_latency = 180
        else:
            lynx_latency = 0

        # Rough 2 second estimation per metric per question
        duration_per_qn_per_metric= (2.7 * len(metrics)) + lynx_latency
        
        # Num of questions
        num_qns = batch_size

        # Num of tasks, subtract away failed ones
        num_tasks = math.ceil(batch_size / max_batch_size) 

        # Estimated total time = Task cold start time + (num_qns x time per qns)/num_tasks
        estimated_time = 370 + int((num_qns * duration_per_qn_per_metric)/num_tasks)

        return estimated_time
    

    ######### MAIN CODE BODY ##########

    # Setting ENV variabeles
    AWS_API_KEY = os.environ.get("AWS_API_KEY")
    PRI_LAMBDA_API = os.environ.get("PRI_LAMBDA_API")
    SEC_LAMBDA_API = os.environ.get("SEC_LAMBDA_API")
    FILE = os.environ.get("FILE")

    print("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"*2)
    print("Preparing nodes for evaluation")
    file_batch, batch_size = encode_excel_to_base64(file)

    # API Header, possible to include aws_api_key here
    headers = {
        'Content-Type': 'application/json',
        'aws-api-key': AWS_API_KEY
        }

    # Get primary lambda response
    primary_response = api_call_primary(file_batch, metrics, headers, batch_size)

    # Extract response to JSON body
    primary_response_body = primary_response.json()  

    # For more than 1 task, if there are failed tasks, print them out
    if primary_response_body['failed_tasks'] > 0:
        print(f"Failed to start {primary_response_body['failed_tasks']} task(s)")

    # Sleep for the estimated buffer time for cold start
    estimated_time = estimate_buffer_time(batch_size, max_batch_size)
    estimated_minutes = math.ceil(estimated_time/60)
    print(f"\033[92mEvaluation in progress. Estimated time to completion: {estimated_minutes} minutes\033[0m")
    time.sleep(estimated_time)
 
    # Start pinging after estimated duration has passed
    secondary_response = api_call_secondary()

    # Extract response JSON body
    results = secondary_response.json()

    #Pop HTML content for separate processing
    if "HTML" in results.keys():
        html_content = results.pop('HTML', '')

    # Print results in the terminal
    print(f"Evaluation completed succesfully. Results:{results}")

    # FOR HTML
    print(f"Refer to the generated report card for a detailed analysis")

    # Create HTML file
    with open('./evaluation_report.html', 'w') as f:
        f.write(html_content)

    return results


# This are function arguments, you can change them directly here or below.
# NOTE that for running multiple tasks, if vCPU, MEM or Storage usage is large, one or more tasks might fail to start
# At other times it might work fine, this is still unstable from current testing

FILE = os.environ.get("FILE")
metrics = ['answer_relevancy']
refreshRate=10
refreshMessage=True
max_batch_size=13

results = evaluateModel(file=FILE,
                        metrics=metrics,
                        refreshRate=refreshRate,
                        refreshMessage=refreshMessage,
                        max_batch_size=max_batch_size
                        )



