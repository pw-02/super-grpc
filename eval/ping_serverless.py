import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json


# Create a Lambda client
lambda_client = boto3.client('lambda')

# Define the function name
function_name = 'superdataloader-CreateBatchFunction-WlTEivefOYJK'

# Define the payload for the Lambda function
payload = {'key': 'value'}

# Number of invocations to perform
num_invocations = 10

# Function to invoke the Lambda function
def invoke_lambda():
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=str(payload)
    )
    # Read the response payload
    return response['Payload'].read().decode()

# Start timing
start_time = time.time()

# Use a thread pool to invoke the Lambda function multiple times
with ThreadPoolExecutor(max_workers=num_invocations) as executor:
    # Submit tasks to the thread pool
    futures = [executor.submit(invoke_lambda) for _ in range(num_invocations)]
    
    # Collect and print results as they complete
    for future in as_completed(futures):
        result = future.result()
        print(f"Lambda response: {result}")

# End timing
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time

# Print the elapsed time
print(f"Elapsed time for invoking Lambda {num_invocations} times using thread pool: {elapsed_time:.2f} seconds")
