from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol
from datetime import datetime

class MRRecords(MRJob):

    OUTPUT_PROTOCOL = TextValueProtocol
    def mapper(self, _, line):
        parts = line.split('|')
        
        # Định dạng thời gian trong chuỗi
        time_format = "%Y-%m-%d %H:%M:%S"

        # Chuyển chuỗi sang datetime
        time_start = datetime.strptime(parts[2], time_format)
        time_end = datetime.strptime(parts[3], time_format)

        # Tính khoảng thời gian
        delta = time_end - time_start

        # Tính số phút
        minutes = int(delta.total_seconds() // 60)
        if parts[-1] == "1" and minutes > 60:
            yield parts[0], minutes

    def reducer(self, key, values):
        yield None, f"{key}\t{list(values)[0]}"

if __name__ == '__main__':
    MRRecords.run()
