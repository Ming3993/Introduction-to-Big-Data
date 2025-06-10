from mrjob.job import MRJob
from mrjob.step import MRStep

class MRHistogram(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.exist_mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.non_exist_reducer)
        ]
    def exist_mapper(self, _, line):
        numbers = sorted(line.strip().split(), key=int)
            
        for num in numbers:
            yield num, 1

    def combiner(self, key, values):
        # Tổng tạm các giá trị (1) trong từng mapper
        yield key, sum(values)

    def reducer(self, key, values):
        # Tổng toàn cục sau combiner
        yield None, (key, sum(values))
        
    def non_exist_reducer(self, _, values):
        hist = {}
        max_hist = -1
        for key, count in values:
            val = int(key)
            hist[val] = count
            yield val, count
            if max_hist < val:
                max_hist = val
        for i in range(max_hist + 1):
            if i not in hist:
                yield i, 0
        
if __name__ == '__main__':
    MRHistogram.run()
