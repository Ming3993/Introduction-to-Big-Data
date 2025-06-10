import tempfile
import json
from word_count import WordCountJob
from itertools import combinations
from collections import defaultdict
from total_transaction import MRApriori
from extract_unique import ExtractUniqueItemsJob

class AprioriRunner:
    def __init__(self, datafile, min_support):
        self.datafile = datafile
        self.min_support = min_support
        self.k = 1
        self.total_transactions = 0
        self.frequent_itemsets = set()
        self.global_frequent = dict()

    def count_total_transactions(self):
        job = MRApriori(args=[self.datafile])
        with job.make_runner() as runner:
            runner.run()
            for key, value in job.parse_output(runner.cat_output()):
                if key == '__total__':
                    self.total_transactions = value

    def get_unique_items(self):
        job = ExtractUniqueItemsJob(args=[self.datafile])
        items = set()
        with job.make_runner() as runner:
            runner.run()
            for key, _ in job.parse_output(runner.cat_output()):
                items.add(key)
        return items

    def generate_candidates(self):
        if self.k == 1:
            items = self.get_unique_items()
            return [frozenset([i]) for i in items]
        else:
            all_items = set(i for fs in self.frequent_itemsets for i in fs)
            candidates = []
            for comb in combinations(sorted(all_items), self.k):
                subsets = combinations(comb, self.k - 1)
                if all(frozenset(s) in self.frequent_itemsets for s in subsets):
                    candidates.append(frozenset(comb))
            return candidates

    def write_candidates_to_file(self, candidates):
        tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        for cand in candidates:
            tmp.write(json.dumps(sorted(cand)) + "\n")
        tmp.close()
        return tmp.name

    def run_iteration(self, candidates_file):
        job = WordCountJob(args=[
            self.datafile,
            f'--candidates={candidates_file}',
            f'--min_support={self.min_support}'
        ])
        counts = defaultdict(int)

        with job.make_runner() as runner:
            runner.run()
            for key, value in job.parse_output(runner.cat_output()):
                if key == "__total__":
                    self.total_transactions = value
                else:
                    counts[frozenset(key)] = value

        new_frequents = set()
        for itemset, count in counts.items():
            support = count / self.total_transactions
            if support >= self.min_support:
                new_frequents.add(itemset)
                self.global_frequent[itemset] = round(support, 3)

        self.frequent_itemsets = new_frequents
        return len(new_frequents) > 0

    def run(self):
        while True:
            print(f"\n==> Iteration k={self.k}")
            candidates = self.generate_candidates()
            if not candidates:
                print("No candidates left.")
                break
            cand_file = self.write_candidates_to_file(candidates)
            has_frequent = self.run_iteration(cand_file)
            if not has_frequent:
                print("No frequent itemsets found.")
                break
            self.k += 1

        print("\n=== Frequent Itemsets ===")
        for itemset, support in sorted(self.global_frequent.items(), key=lambda x: (-len(x[0]), -x[1])):
            print(f"{tuple(itemset)}\t{support}")

if __name__ == "__main__":
    runner = AprioriRunner("./transaction.txt", min_support=0.5)
    runner.run()
