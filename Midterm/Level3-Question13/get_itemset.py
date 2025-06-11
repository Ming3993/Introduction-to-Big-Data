from mrjob.job import MRJob

class GetItemSet(MRJob):
    def mapper(self, _, line):
        try:
            _, items = line.strip().split("\t")
            for item in items.split():
                yield item, None
        except Exception as e:
            import sys
            print(f"Mapper error on line: {line.strip()} | Error: {e}", file=sys.stderr)
            raise e
        
    def combiner(self, key, _):
        yield key, None
        
    def reducer(self, key, _):
        yield key, None

if __name__ == '__main__':
    GetItemSet.run()
