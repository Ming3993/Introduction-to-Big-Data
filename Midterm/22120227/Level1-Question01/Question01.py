from mrjob.job import MRJob
from mrjob.step import MRStep

class ItemTransactionProcessor(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.map_record_to_item,
                combiner=self.combine_item_occurrences,
                reducer=self.reduce_process_transaction
            )
        ]

    def map_record_to_item(self, _, line_content):
        try:
            parts = line_content.strip().split('\t', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                item = parts[1].strip()
                yield key, item
        except ValueError:
            pass

    def combine_item_occurrences(self, transaction_key, item_list):
        temp_counts = {}
        for item_val in item_list:
            temp_counts[item_val] = temp_counts.get(item_val, 0) + 1
        for item_val, count_val in temp_counts.items():
            yield transaction_key, (item_val, count_val)

    def reduce_process_transaction(self, transaction_key, aggregated_item_counts):
        final_item_data = {}
        for item, count in aggregated_item_counts:
            final_item_data[item] = final_item_data.get(item, 0) + count

        sorted_items_output = sorted(final_item_data.items(), key=lambda x: (-x[1], x[0]))
        
        formatted_result = ' '.join([f'[{item}, {count}]' for item, count in sorted_items_output])
        yield transaction_key, formatted_result

if __name__ == '__main__':
    ItemTransactionProcessor.run()