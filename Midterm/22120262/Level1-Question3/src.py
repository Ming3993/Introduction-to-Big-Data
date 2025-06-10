from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

class Question3(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_group),
            MRStep(reducer=self.reducer)
        ]

    def mapper(self, _, line):
        part = line.strip().split('\t')
        if len(part) != 2:
            return

        transaction_id, item = part
        yield item.strip(), transaction_id.strip()

    def reducer_group(self, item, transaction_ids):
        yield item, list(set(transaction_ids))
    
    def reducer(self, item, transaction_id_lists): 
        all_txn_ids = set()
        for txn_list in transaction_id_lists:
            all_txn_ids.update(txn_list)
        if len(all_txn_ids) > 1:
            yield item, "\t" + ", ".join(sorted(all_txn_ids))


if __name__ == '__main__':
    Question3.run()
