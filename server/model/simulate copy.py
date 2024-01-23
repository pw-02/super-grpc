import numpy as np
import matplotlib.pyplot as plt

class OnlineLinearRegression:
    def __init__(self):
        self.n = 0
        self.mean_x = 0
        self.mean_y = 0
        self.mean_xy = 0
        self.mean_xx = 0

    def partial_fit(self, x, y):
        self.n += 1
        delta_x = x - self.mean_x
        delta_y = y - self.mean_y
        self.mean_x += delta_x / self.n
        self.mean_y += delta_y / self.n
        self.mean_xy += delta_x * delta_y
        self.mean_xx += delta_x**2

    def predict(self, x):
        beta = self.mean_xy / self.mean_xx
        alpha = self.mean_y - beta * self.mean_x
        return alpha + beta * x

# Number of batches
num_batches = 200

# Time taken for processing a single batch
time_taken_single_batch = 5

# Noise level
noise_std_dev = 5

# Initialize the online linear regression model
model = OnlineLinearRegression()

# Lists to store data for visualization
actual_batch_access_times = []
predicted_batch_access_times = []

for batch_num in range(1, num_batches + 1):
    # Simulate new data point with noise
    position = batch_num
    batch_access_time = time_taken_single_batch * (position - 1) + np.random.normal(0, noise_std_dev)

    # Update the model with the new data point
    model.partial_fit(position, batch_access_time)

    # Generate test data for visualization
    test_position = position
    predicted_batch_access_time = model.predict(test_position)

    # Store data for visualization
    actual_batch_access_times.append(batch_access_time)
    predicted_batch_access_times.append(predicted_batch_access_time)

# Visualize the results
plt.scatter(range(1, num_batches + 1), actual_batch_access_times, color='black', label='Actual')
plt.plot(range(1, num_batches + 1), predicted_batch_access_times, color='blue', linewidth=3, label='Predicted')
plt.title('Batch Position vs. Predicted Batch Access Time (Online Learning with Noise)')
plt.xlabel('Position of the Batch')
plt.ylabel('Batch Access Time')
plt.legend()

# Show the plot
plt.show()
pass
