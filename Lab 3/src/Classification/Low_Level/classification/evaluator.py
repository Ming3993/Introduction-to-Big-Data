class BinaryClassificationEvaluator:
    def __init__(self, true_label_col, predicted_label_col):
        self.true_label_col = true_label_col
        self.predicted_label_col = predicted_label_col

    def evaluate(self, rdd):
        """
        Evaluate the model using accuracy, precision, recall, and F1 score.
        :param rdd: RDD containing the true and predicted labels
        :return: Dictionary with evaluation metrics
        """
        # Index 0: accuracy, 1: precision, 2: recall, 3: f1_score
        N = rdd.count()  # Total number of samples
        rdd = (
            rdd.map(lambda x: (x[self.true_label_col] == x[self.predicted_label_col], # Map index 0: true = pred
                               x[self.true_label_col] == 1 and x[self.predicted_label_col] == 1, # Map index 1: true = pred = 1
                               x[self.true_label_col] == 0 and x[self.predicted_label_col] == 1, # Map index 2: true = 0, pred = 1
                               x[self.true_label_col] == 1 and x[self.predicted_label_col] == 0)) # Map index 3: true = 1, pred = 0
               .reduce(lambda x, y: (x[0] + y[0], # Reduce index 0: true = pred
                                     x[1] + y[1], # Reduce index 1: true = pred = 1
                                     x[2] + y[2], # Reduce index 2: true = 0, pred = 1
                                     x[3] + y[3])) # Reduce index 3: true = 1, pred = 0
        )
        # Calculate metrics
        accuracy = rdd[0] / N
        precision = rdd[1] / (rdd[1] + rdd[2]) if (rdd[1] + rdd[2]) > 0 else 0
        recall = rdd[1] / (rdd[1] + rdd[3]) if (rdd[1] + rdd[3]) > 0 else 0
        f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        # Return metrics as a dictionary
        return {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1_score
        }