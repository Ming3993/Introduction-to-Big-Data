from mrjob.job import MRJob
import json

class WordCountJob(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--candidates')  # file chứa list itemsets dạng JSON lines
        self.add_passthru_arg('--min_support', type=float)

    def mapper_init(self):
        with open(self.options.candidates) as f:
            self.candidate_sets = [frozenset(json.loads(line)) for line in f]

    def mapper(self, _, line):
        try:
            _, items = line.strip().split("\t")
            transaction = set(items.split())
            for cand in self.candidate_sets:
                if cand.issubset(transaction):
                    yield tuple(sorted(cand)), 1
            yield "__total__", 1
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)
