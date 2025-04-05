from mrjob.job import MRJob
import csv
from datetime import datetime

class Transaction(MRJob):

    def mapper(self, _, line):
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
