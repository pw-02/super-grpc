# import time
# import random

# # Simulated function to populate cache
# def populate_cache(file_size, prefetch_workers):
#     # Simulated delay based on file size and number of workers
#     delay = file_size / (prefetch_workers * 100)  # Arbitrary delay calculation
#     time.sleep(delay)
#     return file_size / delay  # Simulated rate of data population

# # Test parameters
# file_sizes = [2, 50, 500, 1000, 5000]  # File sizes in MB
# prefetch_workers_list = [4, 8, 16, 32, 48]

# # List to store results
# results = []

# # Run the experiment
# for file_size in file_sizes:
#     for workers in prefetch_workers_list:
#         rate = populate_cache(file_size, workers)
#         results.append({
#             'file_size': f'{file_size}MB',
#             'prefetch_workers': workers,
#             'rate': rate
#         })

# # Convert results to DataFrame
# import pandas as pd
# df = pd.DataFrame(results)

# # Save results to CSV
# df.to_csv('prefetch_experiment_results.csv', index=False)

import matplotlib.pyplot as plt
import pandas as pd

# Read results from CSV
df = pd.read_csv('prefetch_experiment_results.csv')

# Set up the plot
plt.figure(figsize=(10, 6))

# Define colors for each file size
colors = ['#F1F1F2', '#767171', '#406474', '#B2C6B6', '#FF6F61']

# Plot data population rate for different file sizes
for i, file_size in enumerate(df['file_size'].unique()):
    subset = df[df['file_size'] == file_size]
    plt.plot(subset['prefetch_workers'], subset['rate'], marker='o', label=f'{file_size}', color=colors[i])

# Customize the plot
plt.xlabel('Number of Prefetch Workers', fontsize=12)
plt.ylabel('Data Population Rate (MB/s)', fontsize=12)
plt.title('Data Population Rate vs. Number of Prefetch Workers for Different File Sizes', fontsize=14)
plt.legend(title='File Size', fontsize=10)
plt.grid(True)
plt.tight_layout()

# Show the plot
plt.show()