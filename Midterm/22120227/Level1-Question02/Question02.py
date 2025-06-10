from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRTransactionTotalCost(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_calc_cost,
                combiner=self.combiner_sum_costs,
                reducer=self.reducer_sum_costs
            )
        ]
    
    def mapper_calc_cost(self, _, line):
        parts = line.rstrip("\n").split("\t", 1)
        if len(parts) != 2:
            return
        key, item = parts
        discount = 0.8 if item.endswith('*') else 1.0
        name = item.rstrip('*').replace(' ', '')
        length = len(re.findall(r'[a-zA-Z0-9]', name))
        cost = length * discount
        yield key, cost

    def combiner_sum_costs(self, key, costs):
        yield key, sum(costs)

    def reducer_sum_costs(self, key, sums_partial):
        total = sum(sums_partial)
        yield key, total

if __name__ == '__main__':
    MRTransactionTotalCost.run()