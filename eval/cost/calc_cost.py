import configparser

# Function to read configuration values from a file
def read_config():
    config = configparser.ConfigParser()
    config.read('costing/config.ini')
    return config

# Function to calculate EC2 costs
def calculate_ec2_cost(instance_type, training_job_duration_hours):
    instance_prices = read_config()['EC2']
    instance_hourly_cost = float(instance_prices.get(instance_type, 0.0))
    return instance_hourly_cost * training_job_duration_hours

# Function to calculate S3 GET request costs
def calculate_s3_get_request_cost(s3_get_request_count):
    s3_config = read_config()['S3']
    s3_get_request_cost_per_thousand = float(s3_config.get('s3_get_request_cost_per_thousand', 0.0))
    return (s3_get_request_count / 1000) * s3_get_request_cost_per_thousand


# Function to calculate Lambda invocation costs
def calculate_lambda_invocation_cost(lambda_invocation_count):
    lambda_config = read_config()['Lambda']
    lambda_invocation_cost = float(lambda_config.get('lambda_invocation_cost', 0.0))
    return lambda_invocation_count * lambda_invocation_cost

# Function to calculate storage costs
def calculate_storage_cost(storage_size_gb, storage_cost_per_gb, storage_duration_months):
    return storage_size_gb * storage_cost_per_gb * storage_duration_months

# Function to calculate data transfer costs
def calculate_data_transfer_cost(data_transfer_volume_gb, data_transfer_cost_per_gb):
    return data_transfer_volume_gb * data_transfer_cost_per_gb

# Function to calculate total costs
def calculate_total_cost(ec2_cost, s3_get_request_cost, lambda_invocation_cost, storage_cost, data_transfer_cost):
    return ec2_cost + s3_get_request_cost + lambda_invocation_cost + storage_cost + data_transfer_cost

# Example values (replace with your actual values)
instance_type = 'g4dn.xlarge'
training_job_duration_hours = 0.5
storage_size_gb = 0
storage_duration_months = 0
data_transfer_volume_gb = 0
s3_get_request_count = 50000  # Replace with your actual S3 GET request count
lambda_invocation_count = 0  # Replace with your actual Lambda invocation count

# Calculate costs
ec2_cost = calculate_ec2_cost(instance_type, training_job_duration_hours)
s3_get_request_cost = calculate_s3_get_request_cost(s3_get_request_count)
lambda_invocation_cost = calculate_lambda_invocation_cost(lambda_invocation_count)
storage_cost = calculate_storage_cost(storage_size_gb, 0.1, storage_duration_months)  # Assuming a fixed storage cost
data_transfer_cost = calculate_data_transfer_cost(data_transfer_volume_gb, 0.02)  # Assuming a fixed data transfer cost

total_cost = calculate_total_cost(ec2_cost, s3_get_request_cost, lambda_invocation_cost, storage_cost, data_transfer_cost)

# Display results
print(f"EC2 Cost for {instance_type}: ${ec2_cost:.2f}")
print(f"S3 GET Request Cost: ${s3_get_request_cost:.2f}")
print(f"Lambda Invocation Cost: ${lambda_invocation_cost:.2f}")
print(f"Storage Cost: ${storage_cost:.2f}")
print(f"Data Transfer Cost: ${data_transfer_cost:.2f}")
print(f"Total Cost: ${total_cost:.2f}")