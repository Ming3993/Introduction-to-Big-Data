from . import math as math
import time

def map_partition(partition, w):
    """
    Map partition function to be used in RDD operations.
    :param partition: a partition of the RDD where each element is a tuple (features, label)
    :param w: weights vector
    :return: a list of tuples (gradient, label)
    """
    for x in partition:
        yield math.derivative(w, x[0], x[1])

class LogisticRegression:
    """
    Logistic Regression Classifier
    :param rdd: RDD of features and labels
    :param learning_rate: learning rate
    :param num_iterations: number of iterations
    :param threshold: threshold for classification
    :param tolerance: tolerance for convergence
    :param checkpoint_interval: interval for checkpointing
    :param feature_index: index of the feature in the RDD
    :param label_index: index of the label in the RDD
    """

    def __init__(self, 
                 learning_rate=0.01, 
                 num_iterations=1000, 
                 batch_size=1000,
                 threshold=0.5, 
                 tolerance=1e-6, 
                 checkpoint_interval=10, 
                 feature_index_train=0,
                 feature_index_test=0, 
                 label_index=1):
        self.learning_rate = learning_rate
        self.num_iterations = num_iterations
        self.tolerance = tolerance
        self.checkpoint_interval = checkpoint_interval
        self.threshold = threshold
        self.feature_index_train = feature_index_train
        self.feature_index_test = feature_index_test
        self.label_index = label_index
        self.batch_size = batch_size
        self.time = 0

    # Logistic regression
    def train(self, rdd):
        """
        Perform training with gradient descent to learn the weights.
        :param rdd: RDD of features and labels
        """
        # Start the timer
        time_start = time.time()

        # Extend the RDD feature vector to include the bias term
        rdd = rdd.map(lambda x: (x[self.feature_index_train], x[self.label_index]))  # Extract features and labels

        # Initialize parameters
        w_prev = None
        w = [0.0] * (len(rdd.first()[0]))  # Initialize weights to zero
        N = rdd.count()  # Number of samples

        # Random split the data
        batch_size = self.batch_size  # Batch size for mini-batch gradient descent
        batch_rdd_size = []
        if N % batch_size == 0:
            batch_rdd_size = [batch_size] * (N // batch_size)
        else:
            batch_rdd_size = [batch_size] * (N // batch_size) + [N % batch_size]

        # Cache the RDD
        rdd = rdd.cache()

        # Training loop
        count_iterations = 0
        isBreak = False

        while count_iterations < self.num_iterations:
            batch_rdd_list = rdd.randomSplit(batch_rdd_size)  # Randomly split the data into batches

            for batch_rdd in batch_rdd_list:
                # Calculate gradients
                gradient = (
                    batch_rdd.mapPartitions(lambda partition: map_partition(partition, w))  # Map partition to calculate gradients
                             .reduce(lambda gradient1, gradient2: math.plus_vector(gradient1, gradient2))  # Sum the gradients
                )

                # Update weights
                w_prev = w.copy()  # Store previous weights for convergence check
                w = math.plus_vector(w, math.multiply_vector_with_scalar(gradient, -self.learning_rate / batch_size))
                
                # Checkpointing
                count_iterations += 1
                if count_iterations % self.checkpoint_interval == 0:
                    print(f"Iteration {count_iterations}: weights = {w}, gradient = {gradient}, distance from previous = {math.distance(w, w_prev)}")
                
                # Check for maximum iterations
                if count_iterations >= self.num_iterations:
                    print(f"Maximum iterations reached: {count_iterations}")
                    isBreak = True
                    break

                # Check for convergence
                if math.distance(w, w_prev) < self.tolerance:
                    print(f"Convergence reached: {math.distance(w, w_prev)}")
                    isBreak = True
                    break
            if isBreak:
                break

        # Unpersist the RDD
        rdd.unpersist()

        # Calculate the time taken for training
        self.time = time.time() - time_start
        print(f"Training time: {self.time:.2f} seconds")
        print(f"Final weights: {w}")
        
        return LogisticRegressionModel(w, self.threshold, self.feature_index_test)

class LogisticRegressionModel:
    """
    Logistic Regression Model
    """
    def __init__(self, w, threshold=0.5, feature_index=0):
        self.w = w
        self.threshold = threshold
        self.feature_index = feature_index
    
    def predict(self, rdd):
        """
        Make predictions using the learned weights. Prediction is added at the end of the RDD.
        :param x: rdd
        :return: predicted label
        """
        return rdd.map(lambda x: 
            x + [1.0 if math.sigmoid(math.dot_product(self.w, x[self.feature_index])) > self.threshold else 0.0]
        )
        
