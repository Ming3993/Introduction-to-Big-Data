from mrjob.job import MRJob

class GetItemSet(MRJob):
    def mapper(self, _, line):
        try:
            _, items = line.strip().split("\t")
            for item in items.split():
                yield item, None
        except:
            pass
        
    def combiner(self, key, _):
        yield key, None
        
    def reducer(self, key, _):
        yield key, None
