from mrjob.job import MRJob

class MRCountNumbers(MRJob):

    def mapper(self, _, line):
        numbers = sorted(line.strip().split(), key=int)
        for num in range(int(numbers[0])):
            yield num, 0
            
        for i in range(len(numbers) - 1):
            num = int(numbers[i])
            yield num, 1
            while (num + 1 < int(numbers[i + 1])):
                num += 1
                yield num, 0
        
        yield int(numbers[-1]), 1

    def combiner(self, key, values):
        # Tổng tạm các giá trị (1) trong từng mapper
        yield key, sum(values)

    def reducer(self, key, values):
        # Tổng toàn cục sau combiner
        yield key, sum(values)

if __name__ == '__main__':
    MRCountNumbers.run()
