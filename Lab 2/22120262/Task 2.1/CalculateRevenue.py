from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime

# Convert string to datetime object
def convert_to_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")

class MRRevenueCalculating(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse,
                   reducer=self.reducer_revenue),
            MRStep(reducer=self.reducer_last_3)
        ]
    def mapper_parse(self, _, line):
        try:
            row = next(csv.reader([line.strip()]))

            if row[0].lower() == "index":
                return

            date_str = row[2]
            status = row[3]
            category = row[9]
            amount = row[15]

            if status.lower() == "cancelled":
                return

            # Parse date
            date = None
            for fmt in ("%m-%d-%y", "%m/%d/%Y"):
                try:
                    date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue

            if not date:
                return

            formatted_date = date.strftime('%d/%m/%Y')
            yield (formatted_date, category), float(amount)

        except Exception:
            pass  # Skip lines with errors

    def reducer_revenue(self, key, values):
        # Merge all data to and assign with key None
        date_str, category = key
        total_revenue = sum(values)
        yield None, (date_str, category, total_revenue)

    def reducer_last_3(self, _, date_category_revenue_pairs):
        # Convert all data into a list
        all_data = list(date_category_revenue_pairs)

        # Convert date_str to datetime for sorting
        all_data_with_datetime = [
            (convert_to_date(date_str), category, revenue)
            for date_str, category, revenue in all_data
        ]

        # Sort by date (Descending)
        all_data_with_datetime.sort(key=lambda x: x[0], reverse=True)
        
        cnt = 0

        # Get the last 3 dates
        for i in range(len(all_data_with_datetime) - 1):
            yesterday, _, _ = all_data_with_datetime[i + 1]
            date_obj, category, revenue = all_data_with_datetime[i]
            if yesterday != date_obj:
                if cnt == 2:
                    break
                else:
                    cnt += 1
            formatted_date = date_obj.strftime('%d/%m/%Y')
            yield (formatted_date, category), revenue
        

if __name__ == '__main__':
    MRRevenueCalculating.run()
