class ZScoreNormalization:
    def __init__(self, column_index):
        self.mean = 0
        self.std = 0
        self.column_index = column_index

    def fit(self, rdd):
        """
        Calculate the mean and standard deviation for Z-score normalization.
        :param rdd: RDD of features
        """
        mean, std = self._calculate_mean_std(rdd, self.column_index)
        return ZScoreNormalizationModel(
                mean, std, self.column_index
        )
    
    def _calculate_mean_std(self, rdd, col_index):
        """
        Calculate the mean and standard deviation for the specified column indices.
        :param rdd: RDD of features (e.g., list or tuple per row)
        :param col_index: Column index for the feature list
        :return: Tuple (means, stds) where each is a list
        """
        def seq_op(acc, row):
            count, sums, sumsqs = acc
            feature_list = row[col_index]
            for i, feature in enumerate(feature_list):
                val = feature
                sums[i] += val
                sumsqs[i] += val * val
            return (count + 1, sums, sumsqs)

        def comb_op(acc1, acc2):
            count1, sums1, sumsqs1 = acc1
            count2, sums2, sumsqs2 = acc2
            combined_sums = [a + b for a, b in zip(sums1, sums2)]
            combined_sumsqs = [a + b for a, b in zip(sumsqs1, sumsqs2)]
            return (count1 + count2, combined_sums, combined_sumsqs)


        num_cols = len(rdd.first()[col_index])
        zero = (0, [0.0] * num_cols, [0.0] * num_cols)

        count, sums, sumsqs = rdd.aggregate(zero, seq_op, comb_op)

        means = [s / count for s in sums]
        stds = [((ss / count) - (m ** 2)) ** 0.5 for ss, m in zip(sumsqs, means)]

        return (means, stds)

    
class ZScoreNormalizationModel:
    def __init__(self, mean, std, column_index):
        """
        Initialize the Z-score normalization model with mean and std.
        :param mean: List mean of the features
        :param std: List std of the features
        :param column_index: Index of the feature column
        """
        self.mean = mean
        self.std = std
        self.column_index = column_index

    def transform(self, rdd):
        """
        Apply Z-score normalization to the RDD.
        :param rdd: RDD of features
        :return: RDD of normalized features
        """
        return rdd.map(lambda x: x + [self._normalize(x)])
    
    def _normalize(self, x):
        """
        Normalize a single feature vector.
        :param x: Feature vector
        :return: Normalized feature vector
        """
        feature = x[self.column_index].copy()  
        for i, val in enumerate(feature):
            if self.std[i] == 0:
                continue
            feature[i] = (val - self.mean[i]) / self.std[i]
        return feature
    
    def inverse_transform(self, rdd):
        """
        Apply inverse Z-score normalization to the RDD.
        :param rdd: RDD of normalized features
        """
        return rdd.map(lambda x: self._inverse_normalize(x))
    
    def _inverse_normalize(self, x):
        """
        Inverse normalize a single feature vector.
        :param x: Normalized feature vector
        :return: Original feature vector
        """
        feature = x[self.column_index]
        for i, val in enumerate(feature):
            feature[i] = (val * self.std[i]) + self.mean[i]
        return feature