from sklearn.linear_model import SGDRegressor
import numpy as np

class RealTimeModel:
    def __init__(self, feature_dim):
        self.model = SGDRegressor()
        self.feature_dim = feature_dim

    def update_model(self, job_id, batch_id, pending_batches_count, processing_speed, actual_time):
        # Extract features from the data
        features = np.array([job_id, batch_id, processing_speed, pending_batches_count])

        # Ensure that the feature dimensions match
        assert features.shape[0] == self.feature_dim

        # Update the model with the new data using partial_fit
        self.model.partial_fit(features.reshape(1, -1), np.array([actual_time]))

    def make_prediction(self, job_id, batch_id,processing_speed, pending_batches_count):
        # Make predictions using the updated model
        input_features = np.array([job_id, batch_id, processing_speed, pending_batches_count])
        return self.model.predict(input_features.reshape(1, -1))[0]
