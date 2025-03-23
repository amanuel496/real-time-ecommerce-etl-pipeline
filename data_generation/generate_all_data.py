import csv
import random
import uuid
import argparse
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path

# --- Argument Parser ---
parser = argparse.ArgumentParser(description="Generate sample e-commerce data.")
parser.add_argument("--customers", type=int, default=100, help="Number of customers")
parser.add_argument("--products", type=int, default=50, help="Number of products")
parser.add_argument("--suppliers", type=int, default=10, help="Number of suppliers")
parser.add_argument("--promotions", type=int, default=20, help="Number of promotions")
parser.add_argument("--orders", type=int, default=200, help="Number of orders")
args = parser.parse_args()

# --- Config from CLI ---
NUM_CUSTOMERS = args.customers
NUM_PRODUCTS = args.products
NUM_SUPPLIERS = args.suppliers
NUM_PROMOTIONS = args.promotions
NUM_ORDERS = args.orders

fake = Faker()
output_dir = Path("sample_data")
output_dir.mkdir(exist_ok=True)

# --- Helpers ---
def write_csv(filename, headers, rows):
    with open(output_dir / filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

# --- 1. Customers ---
customers = [
    [i, fake.name(), fake.email(), fake.address().replace("\n", ", "), fake.date_between("-1y", "today")]
    for i in range(1, NUM_CUSTOMERS + 1)
]
write_csv("customers.csv", ["CustomerID", "Name", "Email", "Address", "SignupDate"], customers)

# --- 2. Suppliers ---
suppliers = [
    [i, fake.company(), fake.phone_number()]
    for i in range(1, NUM_SUPPLIERS + 1)
]
write_csv("suppliers.csv", ["SupplierID", "Name", "ContactInfo"], suppliers)

# --- 3. Products ---
products = [
    [i, fake.word().capitalize(), random.choice(["Electronics", "Clothing", "Books", "Home", "Beauty"]),
     round(random.uniform(5, 500), 2)]
    for i in range(1, NUM_PRODUCTS + 1)
]
write_csv("products.csv", ["ProductID", "Name", "Category", "Price"], products)

# --- 4. ProductSuppliers ---
product_suppliers = [
    [i, i, random.randint(1, NUM_SUPPLIERS)]
    for i in range(1, NUM_PRODUCTS + 1)
]
write_csv("product_suppliers.csv", ["ProductSupplierID", "ProductID", "SupplierID"], product_suppliers)

# --- 5. Promotions ---
promotions = []
for i in range(1, NUM_PROMOTIONS + 1):
    start = fake.date_between("-6M", "-1M")
    end = start + timedelta(days=random.randint(5, 30))
    promotions.append([
        i,
        round(random.uniform(5, 50), 2),
        start,
        end,
        random.choice(["Holiday Sale", "Flash Deal", "New Customer"])
    ])
write_csv("promotions.csv", ["PromoID", "Discount", "StartDate", "EndDate", "CampaignType"], promotions)

# --- 6–11: Orders and Linked Tables ---
orders = []
order_details = []
order_promotions = []
payments = []
shipments = []
order_status_history = []

order_id_counter = 1
order_detail_id = 1
order_promo_id = 1
payment_id = 1
shipment_id = 1
status_id = 1

for i in range(1, NUM_ORDERS + 1):
    cust_id = random.randint(1, NUM_CUSTOMERS)
    order_date = fake.date_between("-6M", "today")
    status = random.choice(["Pending", "Shipped", "Delivered", "Canceled"])
    total = 0
    product_ids = random.sample(range(1, NUM_PRODUCTS + 1), random.randint(1, 3))

    # Orders
    orders.append([order_id_counter, cust_id, order_date, 0, status])

    # OrderDetails
    for pid in product_ids:
        qty = random.randint(1, 4)
        price = next(p[3] for p in products if p[0] == pid)
        subtotal = round(qty * price, 2)
        order_details.append([order_detail_id, order_id_counter, pid, qty, subtotal])
        total += subtotal
        order_detail_id += 1

    # OrderPromotions
    if random.random() < 0.4:
        promo = random.choice(promotions)
        order_promotions.append([order_promo_id, order_id_counter, promo[0], promo[1]])
        total -= promo[1]
        order_promo_id += 1

    # Payments
    payments.append([
        payment_id, order_id_counter,
        random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
        random.choice(["Paid", "Pending", "Refunded"]),
        round(total, 2)
    ])
    payment_id += 1

    # Shipments
    shipments.append([
        shipment_id, order_id_counter, fake.company(),
        str(uuid.uuid4())[:8], status,
        order_date + timedelta(days=random.randint(2, 7))
    ])
    shipment_id += 1

    # Status History
    date_progress = order_date
    for s in ["Pending", "Shipped", "Delivered"]:
        if random.random() < 0.9:
            order_status_history.append([status_id, order_id_counter, s, date_progress])
            status_id += 1
            date_progress += timedelta(days=random.randint(1, 3))
        if s == status:
            break

    orders[-1][3] = round(total, 2)
    order_id_counter += 1

write_csv("orders.csv", ["OrderID", "CustomerID", "OrderDate", "TotalAmount", "Status"], orders)
write_csv("order_details.csv", ["OrderDetailID", "OrderID", "ProductID", "Quantity", "Subtotal"], order_details)
write_csv("order_promotions.csv", ["OrderPromotionID", "OrderID", "PromotionID", "DiscountApplied"], order_promotions)
write_csv("payments.csv", ["PaymentID", "OrderID", "PaymentMethod", "PaymentStatus", "Amount"], payments)
write_csv("shipments.csv", ["ShipmentID", "OrderID", "Carrier", "TrackingNumber", "Status", "DeliveryDate"], shipments)
write_csv("order_status_history.csv", ["OrderStatusID", "OrderID", "Status", "StatusDate"], order_status_history)

# --- 12. Inventory ---
inventory = [
    [i, i, random.randint(0, 1000), fake.date_time_this_month()]
    for i in range(1, NUM_PRODUCTS + 1)
]
write_csv("inventory.csv", ["InventoryID", "ProductID", "StockLevel", "LastUpdated"], inventory)