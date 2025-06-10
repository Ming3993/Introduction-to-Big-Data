from mrjob.job import MRJob
from mrjob.step import MRStep

class MRApriori(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--min_support', type=float, default=0.5)

    def load_args(self, args):
        super().load_args(args)
        self.k = 1  # Itemset size
        self.total_transactions = None
        self.frequent_itemsets = set()

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_transactions,
                   reducer=self.reducer_sum_transactions)
        ]

    def mapper_count_transactions(self, _, line):
        yield '__total__', 1

    def reducer_sum_transactions(self, key, values):
        total = sum(values)
        self.total_transactions = total
        yield key, total
