from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

class Question4(MRJob):
    OUTPUT_PROTOCOL = RawProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        part = line.strip().split('\t')
        if len(part) < 2:
            return
        group_name, score = part
        yield group_name.strip(), int(score)

    def reducer(self, group_name, scores):
        scores = list(scores)
        scores.sort()
        if not scores:
            return
        center = round(sum(scores) / len(scores), 2)
        centroid = scores[len(scores) // 2]

        yield group_name, f"\t{center}\t{centroid}"

if __name__ == '__main__':
    Question4.run()