from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import matplotlib.pyplot as plt

# Simulated data (replace this with your actual data)
batch_numbers = np.arange(1, 101)
base_time_per_batch = 5  # Example average processing time without delays
random_delays = np.random.normal(loc=0, scale=25, size=len(batch_numbers))  # Example random delays

# Creating features and labels
features = batch_numbers.reshape(-1, 1)
labels = base_time_per_batch * batch_numbers + random_delays

# Splitting the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)

# Training the linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Making predictions on the test set
predictions = model.predict(X_test)

# Evaluating the model
mse = mean_squared_error(y_test, predictions)
print(f'Mean Squared Error: {mse}')

# Example prediction for a new batch
new_batch_number = 110
predicted_time = model.predict(np.array([[new_batch_number]]))
print(f'Predicted Time for Batch {new_batch_number}: {predicted_time[0]}')

# Plotting the actual vs. predicted processing times
plt.figure(figsize=(10, 6))
plt.scatter(X_test, y_test, color='blue', label='Actual Processing Times')
plt.plot(X_test, predictions, color='red', linewidth=2, label='Predicted Processing Times')
plt.xlabel('Batch Number')
plt.ylabel('Processing Time')
plt.title('Actual vs. Predicted Processing Times')
plt.legend()
plt.show()

pass