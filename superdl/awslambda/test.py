import json
from batch_creation.app import lambda_handler
test_data_str = """
{
  "bucket_name": "sdl-cifar10",
  "batch_id": 1279,
  "batch_samples": [
    [
      "train/Airplane/attack_aircraft_s_001210.png",
      0
    ],
    [
      "train/Airplane/attack_aircraft_s_001210.png",
      0
    ],
    [
      "train/Airplane/attack_aircraft_s_001210.png",
      0
    ],
    [
      "train/Airplane/attack_aircraft_s_001210.png",
      0
    ],
    [
      "train/Airplane/attack_aircraft_s_001210.png",
      0
    ]
  ],
  "cache_address": "localhost:6379",
  "task": "vision"
}
"""

# Convert JSON data string to JSON object
test_data = json.loads(test_data_str)

# Now you can work with test_data as a JSON object
print(test_data)

lambda_handler(test_data, None)