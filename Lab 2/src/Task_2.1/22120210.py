from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol
import csv
from datetime import datetime, timedelta

# Convert string to datetime object
def convert_to_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")

class MRRevenueCalculating(MRJob):
    # Format the output to write into a .csv file
    OUTPUT_PROTOCOL = TextValueProtocol

    # Define step for MapReduce programme
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse,
                   reducer=self.reducer_revenue),
            MRStep(reducer=self.reducer_last_3)
        ]
        
    def mapper_parse(self, _, line):
        try:
            row = next(csv.reader([line.strip()]))
            
            # Skip the header row
            if row[0].lower() == "index":
                return

            date_str = row[2]
            status = row[3]
            category = row[9]
            amount = float(row[15])

            # Skip entries with status "cancelled"
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

            # Yield revenue for the current day and the next two days
            for _ in range(3):
                formatted_date = date.strftime('%d/%m/%Y')
                yield (formatted_date, category), amount
                date = date + timedelta(days=1)

        except Exception:
            pass  # Skip lines with parsing errors

    def reducer_revenue(self, key, values):
        # Aggregate revenue by date and category, yield with key None
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

        # Sort data by date (descending) and then by category name
        all_data_with_datetime.sort(key=lambda x: (x[0], x[1].lower()))

        # Yield formatted output
        yield None,"report_date,category,revenue"
        for date_obj, category, revenue in all_data_with_datetime:
            formatted_date = date_obj.strftime('%d/%m/%Y')
            yield None,f"{formatted_date},{category},{revenue:.2f}"


if __name__ == '__main__':
    MRRevenueCalculating.run()
