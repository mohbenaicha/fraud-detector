import os
from typing import List
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")

gbq_client = bigquery.Client(project=PROJECT_ID)
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
table_ref = dataset_ref.table(TABLE_ID)

try:
    dataset = gbq_client.create_dataset(dataset_ref)
except Exception as e:
    print(f"Dataset may already exist: {e}")


def create_table_if_not_exists(client: bigquery.Client, table_ref: str, schema: List[bigquery.SchemaField]):
    try:
        table = client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        try:
            table = client.create_table(table)
        except Exception as e:
            print(f"Failed to create table: {e}")

def get_rows(client, table_ref):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    query_job = client.query(query)
    query_job.result()


# 2 Define table schema matching your DataFrame
schema = [
    bigquery.SchemaField("TransactionID", "STRING"),
    bigquery.SchemaField("Transaction Amount", "FLOAT"),
    bigquery.SchemaField("Quantity", "FLOAT"),
    bigquery.SchemaField("Customer Age", "FLOAT"),
    bigquery.SchemaField("Account Age Days", "FLOAT"),
    bigquery.SchemaField("Transaction Hour", "FLOAT"),
    bigquery.SchemaField("Transaction Amount_missing", "INT64"),
    bigquery.SchemaField("Quantity_missing", "INT64"),
    bigquery.SchemaField("Customer Age_missing", "INT64"),
    bigquery.SchemaField("Account Age Days_missing", "INT64"),
    bigquery.SchemaField("Transaction Hour_missing", "INT64"),
    bigquery.SchemaField("Payment Method_missing", "INT64"),
    bigquery.SchemaField("Product Category_missing", "INT64"),
    bigquery.SchemaField("Device Used_missing", "INT64"),
    bigquery.SchemaField("Amount_x_AccountAge", "FLOAT"),
    bigquery.SchemaField("Quantity_per_AccountAge", "FLOAT"),
    bigquery.SchemaField("Transaction_DayNight", "INT64"),
    bigquery.SchemaField("Log_Transaction_Amount", "FLOAT"),
    bigquery.SchemaField("Payment Method_bank transfer", "FLOAT"),
    bigquery.SchemaField("Payment Method_credit card", "FLOAT"),
    bigquery.SchemaField("Payment Method_debit card", "FLOAT"),
    bigquery.SchemaField("Product Category_electronics", "FLOAT"),
    bigquery.SchemaField("Product Category_health & beauty", "FLOAT"),
    bigquery.SchemaField("Product Category_home & garden", "FLOAT"),
    bigquery.SchemaField("Product Category_toys & games", "FLOAT"),
    bigquery.SchemaField("Device Used_mobile", "FLOAT"),
    bigquery.SchemaField("Device Used_tablet", "FLOAT"),
]

def test_bigquery_service():
    test_row = [
        {
            "TransactionID": "test_001",
            "Transaction Amount": 123.45,
            "Quantity": 2,
            "Customer Age": 35,
            "Account Age Days": 450,
            "Transaction Hour": 14,
            "Transaction Amount_missing": 0,
            "Quantity_missing": 0,
            "Customer Age_missing": 0,
            "Account Age Days_missing": 0,
            "Transaction Hour_missing": 0,
            "Payment Method_missing": 0,
            "Product Category_missing": 0,
            "Device Used_missing": 0,
            "Amount_x_AccountAge": 0.274,
            "Quantity_per_AccountAge": 0.004,
            "Transaction_DayNight": 1,
            "Log_Transaction_Amount": 4.812,
            "Payment Method_bank transfer": 0,
            "Payment Method_credit card": 1,
            "Payment Method_debit card": 0,
            "Product Category_electronics": 1,
            "Product Category_health & beauty": 0,
            "Product Category_home & garden": 0,
            "Product Category_toys & games": 0,
            "Device Used_mobile": 1,
            "Device Used_tablet": 0,
        }
    ]

    gbq_client.insert_rows_json(table_ref, test_row)
    print("Test row written successfully.")

    # Read it back
    # query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` WHERE TransactionID='test_001'"
    # result = client.query(query).result()
    # for row in result:
    #     print("Retrieved row:", dict(row))


# Run the test
if __name__ == "__main__":
    create_table_if_not_exists(gbq_client, table_ref, schema)
    test_bigquery_service()