import os
import csv
import random
from datetime import datetime

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
file_location = "C:\\Users\\HP\\Downloads\\Fulltime\\June 2024\\sales_data_to_s3"

if not os.path.exists(file_location):
    os.makedirs(file_location)

input_date_str = input("Enter the date for which you want to generate (YYYY-MM-DD): ")
input_date = datetime.strptime(input_date_str, "%Y-%m-%d")

csv_file_path = os.path.join(file_location, f"less_column_data.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "payment_mode"])

    for _ in range(1000):
        customer_id = random.choice(customer_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = input_date
        sales_person_id = random.choice(list(sales_persons.values()))
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity
        payment_mode = random.choice(["cash", "UPI"])

        csvwriter.writerow(
            [customer_id, product_name, sales_date.strftime("%Y-%m-%d"), sales_person_id, price, quantity, total_cost, payment_mode])

    print("CSV file generated successfully:", csv_file_path)
