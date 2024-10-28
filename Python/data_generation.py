import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime


fake = Faker()


def generate_customers(n):
    df = pd.DataFrame(
        {
            "customer_id": range(1, n + 1),
            "name": [fake.name() for _ in range(n)],
            "email": [fake.email() for _ in range(n)],
            "location": [fake.city() for _ in range(n)],
            "age": np.random.randint(18, 80, n),
            "registration_date": [
                fake.date_between(start_date="-5y", end_date="today") for _ in range(n)
            ],
        }
    )
    df["segment"] = pd.cut(
        df["age"], bins=[0, 30, 50, 100], labels=["Young", "Middle", "Senior"]
    )
    return df


def generate_products(n):
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
    names = [f"{fake.word()} {fake.word()}" for _ in range(n)]
    df = pd.DataFrame(
        {
            "product_id": range(1, n + 1),
            "name": names,
            "category": [random.choice(categories) for _ in range(n)],
            "price": np.random.uniform(10, 1000, n).round(2),
            "stock_quantity": np.random.randint(0, 1000, n),
        }
    )
    # Create some product pairs for affinity analysis
    df.loc[df.index[::20], "affinity_group"] = range(1, n // 20 + 1)
    df.loc[df.index[1::20], "affinity_group"] = range(1, n // 20 + 1)
    return df


def generate_transactions(n, customers, products):
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()

    # Create base transactions
    transactions = pd.DataFrame(
        {
            "transaction_id": range(1, n + 1),
            "customer_id": np.random.choice(customers["customer_id"], n),
            "product_id": np.random.choice(products["product_id"], n),
            "quantity": np.random.randint(1, 10, n),
            "timestamp": [
                fake.date_time_between(start_date=start_date, end_date=end_date)
                for _ in range(n)
            ],
        }
    )

    # Merge with products and customers
    transactions = transactions.merge(
        products[["product_id", "price", "category"]], on="product_id"
    )
    transactions = transactions.merge(
        customers[["customer_id", "segment"]], on="customer_id"
    )

    # Convert timestamp to datetime if it isn't already
    transactions["timestamp"] = pd.to_datetime(transactions["timestamp"])
    transactions["month"] = transactions["timestamp"].dt.month

    # Create multiplier column initialized to 1
    transactions["seasonal_multiplier"] = 1.0

    # Apply seasonal patterns
    mask_electronics = transactions["category"] == "Electronics"
    mask_clothing = transactions["category"] == "Clothing"
    mask_sports = transactions["category"] == "Sports"

    mask_electronics_season = transactions["month"].isin([11, 12])
    mask_clothing_season = transactions["month"].isin([3, 4, 5, 9, 10, 11])
    mask_sports_season = transactions["month"].isin([6, 7, 8])

    # Update multipliers based on category and season
    transactions.loc[
        mask_electronics & mask_electronics_season, "seasonal_multiplier"
    ] = 1.5
    transactions.loc[mask_clothing & mask_clothing_season, "seasonal_multiplier"] = 1.3
    transactions.loc[mask_sports & mask_sports_season, "seasonal_multiplier"] = 1.4

    # Apply customer segment preferences
    # segment_multiplier = 1.0
    transactions.loc[
        (transactions["segment"] == "Young")
        & (transactions["category"].isin(["Electronics", "Sports"])),
        "seasonal_multiplier",
    ] *= 1.2
    transactions.loc[
        (transactions["segment"] == "Middle")
        & (transactions["category"].isin(["Home", "Books"])),
        "seasonal_multiplier",
    ] *= 1.2
    transactions.loc[
        (transactions["segment"] == "Senior")
        & (transactions["category"].isin(["Books", "Clothing"])),
        "seasonal_multiplier",
    ] *= 1.2

    # Apply multipliers to quantity
    transactions["quantity"] = (
        (transactions["quantity"] * transactions["seasonal_multiplier"])
        .round()
        .astype(int)
    )

    # Calculate total amount
    transactions["total_amount"] = transactions["quantity"] * transactions["price"]

    # Drop unnecessary columns
    transactions = transactions.drop(
        ["price", "month", "segment", "seasonal_multiplier"], axis=1
    )

    return transactions


def generate_clickstream(n, customers, products):
    actions = ["view", "add_to_cart", "remove_from_cart", "purchase"]
    df = pd.DataFrame(
        {
            "click_id": range(1, n + 1),
            "customer_id": np.random.choice(customers["customer_id"], n),
            "product_id": np.random.choice(products["product_id"], n),
            "timestamp": [fake.date_time_this_year() for _ in range(n)],
            "action": np.random.choice(actions, n, p=[0.7, 0.15, 0.05, 0.1]),
        }
    )
    # Ensure higher view and add_to_cart actions lead to more purchases
    purchase_prob = df.groupby("customer_id")["action"].apply(
        lambda x: (x == "view").mean() + (x == "add_to_cart").mean()
    )
    purchase_prob = (purchase_prob - purchase_prob.min()) / (
        purchase_prob.max() - purchase_prob.min()
    )
    df.loc[df["action"] == "purchase", "action"] = np.where(
        np.random.random(len(df[df["action"] == "purchase"]))
        < purchase_prob[df[df["action"] == "purchase"]["customer_id"]],
        "purchase",
        "view",
    )
    return df


def generate_support_tickets(n, customers):
    categories = ["Product Issue", "Shipping", "Return", "Account", "Other"]
    statuses = ["Open", "In Progress", "Closed"]
    df = pd.DataFrame(
        {
            "ticket_id": range(1, n + 1),
            "customer_id": np.random.choice(customers["customer_id"], n),
            "category": np.random.choice(categories, n),
            "status": np.random.choice(statuses, n),
            "created_at": [fake.date_time_this_year() for _ in range(n)],
            "resolved_at": [
                fake.date_time_this_year() if random.random() > 0.3 else None
                for _ in range(n)
            ],
        }
    )
    # Customers with more tickets are more likely to churn
    df["churn_risk"] = df.groupby("customer_id")["ticket_id"].transform("count")
    df["churn_risk"] = (df["churn_risk"] - df["churn_risk"].min()) / (
        df["churn_risk"].max() - df["churn_risk"].min()
    )
    return df


def generate_marketing_campaigns(n):
    channels = ["Email", "Social Media", "Search Engine", "Display Ads"]
    df = pd.DataFrame(
        {
            "campaign_id": range(1, n + 1),
            "name": [f"Campaign_{i}" for i in range(1, n + 1)],
            "channel": np.random.choice(channels, n),
            "start_date": [fake.date_this_year() for _ in range(n)],
            "end_date": [fake.date_this_year() for _ in range(n)],
            "budget": np.random.uniform(1000, 50000, n).round(2),
        }
    )
    # Email campaigns have higher short-term effectiveness
    df["short_term_effectiveness"] = np.where(
        df["channel"] == "Email",
        np.random.uniform(0.8, 1.0, n),
        np.random.uniform(0.5, 0.8, n),
    )
    # Search Engine campaigns have higher long-term effectiveness
    df["long_term_effectiveness"] = np.where(
        df["channel"] == "Search Engine",
        np.random.uniform(0.8, 1.0, n),
        np.random.uniform(0.5, 0.8, n),
    )
    return df


# Generate datasets
customers = generate_customers(10000)
products = generate_products(1000)
transactions = generate_transactions(100000, customers, products)
clickstream = generate_clickstream(500000, customers, products)
support_tickets = generate_support_tickets(5000, customers)
marketing_campaigns = generate_marketing_campaigns(50)

# Save to CSV
customers.to_csv("customers.csv", index=False)
products.to_csv("products.csv", index=False)
transactions.to_csv("transactions.csv", index=False)
clickstream.to_csv("clickstream.csv", index=False)
support_tickets.to_csv("support_tickets.csv", index=False)
marketing_campaigns.to_csv("marketing_campaigns.csv", index=False)

print(
    "Data generation complete. CSV files have been created with discoverable patterns."
)
