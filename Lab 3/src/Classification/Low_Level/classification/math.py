import math

def plus_vector(a, b):
    """
    Add two vectors element-wise.
    :param a: first vector
    :param b: second vector
    :return: sum of the two vectors
    """
    if len(a) != len(b):
        raise ValueError("Vectors must be of the same length")
    return [a_i + b_i for a_i, b_i in zip(a, b)]

def multiply_vector_with_scalar(a, b):
    """
    Multiply a vector by a scalar.
    :param a: vector
    :param b: scalar
    :return: product of the vector and scalar
    """
    return [a_i * b for a_i in a]

def sigmoid(x):
    """
    Compute the sigmoid function.
    :param x: float scalar
    :return: sigmoid of x
    """
    return 1 / (1 + math.exp(-x))

def dot_product(w, x):
    """
    Calculate the dot product of two vectors.
    :param w: weights vector
    :param x: features vector
    :return: dot product
    """
    if len(w) != len(x):
        raise ValueError("Vectors must be of the same length")
    return sum(a * b for a, b in zip(w, x))

def derivative(w, x, y):
    """
    Calculate the derivative of the loss function.
    :param w: weights vector
    :param x: features vector
    :param y: label
    :return: vector derivative of the loss function
    """
    y_pred = sigmoid(dot_product(w, x))
    return multiply_vector_with_scalar(
        x, y_pred - y
    )  # Gradient of the loss function
    
def distance(a, b):
    """
    Calculate the Euclidean distance between two vectors.
    :param a: first vector
    :param b: second vector
    :return: Euclidean distance
    """
    return math.sqrt(sum((a_i - b_i) ** 2 for a_i, b_i in zip(a, b)))
