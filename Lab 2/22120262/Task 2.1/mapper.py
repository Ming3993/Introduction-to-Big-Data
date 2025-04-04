import sys
import csv
from datetime import datetime

for line in sys.stdin:
    try:
        row = next(csv.reader([line]))

        if row[0] == "index":
            continue

        date_str = row[2]
        status = row[3]
        category = row[9]
        amount = row[15]

        if status == "Cancelled":
            continue

        date = None
        try:
            date = datetime.strptime(date_str, "%m-%d-%y")
        except ValueError:
            try:
                date = datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                continue

        print(f"{date.strftime("%d/%m/%Y")}\t{category}\t{amount}")

    except Exception as e:
        continue  # Bỏ qua dòng lỗi