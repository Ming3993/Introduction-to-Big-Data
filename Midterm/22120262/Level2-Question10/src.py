from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol


class RightOuterJoin(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        parts = line.strip().split(' ')
        if len(parts) != 3:
            return
        table, key, value = parts
        if table == "FoodPrice":
            yield key, ("P", value)
        elif table == "FoodQuantity":
            yield key, ("Q", value)

    def reducer(self, key, values):
        price = None
        quantity_list = []

        for tag, value in values:
            if tag == "P":
                price = value
            elif tag == "Q":
                quantity_list.append(value)

        for quantity in quantity_list:
            
            price_quantity_str = f"{price if price else 'null'} {quantity}"
            yield key, price_quantity_str

if __name__ == '__main__':
    RightOuterJoin.run()