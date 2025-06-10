from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

class MRJoin(MRJob):
    OUTPUT_PROTOCOL = RawProtocol
    def mapper(self, _, line):
        parts = line.strip().split()
        
        if parts[0] == "FoodPrice":
            yield parts[1], f"{parts[2]} null"
            
        if parts[0] == "FoodQuantity":
            yield parts[1], f"null {parts[2]}"

    def combiner(self, key, values):
        item = list(values)
        
        if len(item) == 2:
            left = item[0].strip().split()
            right = item[1].strip().split()
            
            if left[0] == "null":
                yield key, f"{left[1]} {right[0]}"
            else:
                yield key, f"{left[0]} {right[1]}"
        else:
            yield key, f"{item[0]}"

    def reducer(self, key, values):
        item = list(values)
        
        if len(item) == 2:
            left = item[0].strip().split()
            right = item[1].strip().split()
            
            if left[0] == "null":
                yield key, f"{left[1]} {right[0]}"
            else:
                yield key, f"{left[0]} {right[1]}"
        else:
            yield key, f"{item[0]}"

if __name__ == '__main__':
    MRJoin.run()
