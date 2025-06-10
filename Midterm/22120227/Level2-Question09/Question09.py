from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class MRLeftJoinFood(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        parts = line.strip().split()
        if len(parts) != 3:
            return
        table, key, value = parts
        # Gán tag 'P' cho price, 'Q' cho quantity
        if table == "FoodPrice":
            yield key, ("P", value)
        elif table == "FoodQuantity":
            yield key, ("Q", value)

    def reducer(self, key, records):
        price = None
        quantity = "null"
        found_price = False

        for tag, val in records:
            if tag == "P":
                price = val
                found_price = True
            elif tag == "Q":
                quantity = val

        if found_price:
            yield None, f"{key} {price} {quantity}"

if __name__ == '__main__':
    MRLeftJoinFood.run()