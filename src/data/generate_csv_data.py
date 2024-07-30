import csv
import os
import random
from datetime import datetime, timedelta

customer_ids = list(range(1, 51))
store_ids = list(range(121, 126))
product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "rice": 20,
    "flour": 52,
    "refined oil": 110,
    "shampoo": 15,
    "toothpaste": 10,
    "nutella": 40,
    "chips":20,
    "bread": 25,
    "pasta": 18,
    "milk": 5.5,
    "chocolate":3.2,
    "popcorn": 1.5
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9],
    124: [10, 11, 12],
    125: [13, 14, 15]
}

start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 7, 1)

file_location = "C:\\Users\\HP\\Downloads\\Fulltime\\June 2024\\sales_data_to_s3"
csv_file_path = os.path.join(file_location, "sales_data.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost"])

    for _ in range(500000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity

        csvwriter.writerow([customer_id, store_id, product_name, sales_date.strftime("%Y-%m-%d"), sales_person_id, price, quantity, total_cost])

print("CSV file generated successfully.")
