import time
import os
from dotenv import load_dotenv
from google.cloud import aiplatform

load_dotenv()
PROJECT_ID = os.environ.get("PROJECT_ID")
LOCATION = os.environ.get("LOCATION")
FEATURESTORE_ID = os.environ.get("FEATURESTORE_ID")
ENTITY_TYPE_ID = os.environ.get("ENTITY_TYPE_ID")

# Feature schema mapping: raw column: (sanitized feature_id, value_type)
FEATURES_SCHEMA = {
    "Transaction Amount": ("transaction_amount", "DOUBLE"),
    "Quantity": ("quantity", "DOUBLE"),
    "Customer Age": ("customer_age", "DOUBLE"),
    "Account Age Days": ("account_age_days", "DOUBLE"),
    "Transaction Hour": ("transaction_hour", "DOUBLE"),
    "Transaction_DayNight": ("transaction_daynight", "INT64"),
    # Missing indicators
    "Transaction Amount_missing": ("transaction_amount_missing", "INT64"),
    "Quantity_missing": ("quantity_missing", "INT64"),
    "Customer Age_missing": ("customer_age_missing", "INT64"),
    "Account Age Days_missing": ("account_age_days_missing", "INT64"),
    "Transaction Hour_missing": ("transaction_hour_missing", "INT64"),
    "Payment Method_missing": ("payment_method_missing", "INT64"),
    "Product Category_missing": ("product_category_missing", "INT64"),
    "Device Used_missing": ("device_used_missing", "INT64"),
    # One-hot columns
    "Payment Method_bank transfer": ("payment_method_bank_transfer", "DOUBLE"),
    "Payment Method_credit card": ("payment_method_credit_card", "DOUBLE"),
    "Payment Method_debit card": ("payment_method_debit_card", "DOUBLE"),
    "Product Category_electronics": ("product_category_electronics", "DOUBLE"),
    "Product Category_health & beauty": ("product_category_health_beauty", "DOUBLE"),
    "Product Category_home & garden": ("product_category_home_garden", "DOUBLE"),
    "Product Category_toys & games": ("product_category_toys_games", "DOUBLE"),
    "Device Used_mobile": ("device_used_mobile", "DOUBLE"),
    "Device Used_tablet": ("device_used_tablet", "DOUBLE"),
    # Engineered
    "Amount_x_AccountAge": ("amount_x_accountage", "DOUBLE"),
    "Quantity_per_AccountAge": ("quantity_per_accountage", "DOUBLE"),
    "Log_Transaction_Amount": ("log_transaction_amount", "DOUBLE"),
}

aiplatform.init(project=PROJECT_ID, location=LOCATION)
featurestore_name = (
    f"projects/{PROJECT_ID}/locations/{LOCATION}/featurestores/{FEATURESTORE_ID}"
)

try:
    fstore = aiplatform.Featurestore.create(
        featurestore_id=FEATURESTORE_ID, online_store_fixed_node_count=1
    )
except Exception as e:
    fstore = aiplatform.Featurestore(featurestore_name=featurestore_name)
    time.sleep(2)  # alloww  featurestore to be ready

try:
    entity_type = aiplatform.EntityType.create(
        featurestore_name=featurestore_name,
        entity_type_id=ENTITY_TYPE_ID,
    )
except Exception as e:
    entity_type_name = f"{featurestore_name}/entityTypes/{ENTITY_TYPE_ID}"
    entity_type = aiplatform.EntityType(
        entity_type_name=f"{featurestore_name}/entityTypes/{ENTITY_TYPE_ID}"
    )

feature_configs = {
    fid: {"value_type": ftype} for _, (fid, ftype) in FEATURES_SCHEMA.items()
}

# Delete if they exist; TODO: handle deletion more gracefully
for fid, _ in FEATURES_SCHEMA.values():
    try:
        feature = aiplatform.Feature(
            feature_name=f"{featurestore_name}/entityTypes/{ENTITY_TYPE_ID}/features/{fid}"
        )
        feature.delete()
    except Exception as e:
        print(f"Skip delete {fid}: {e}")


try:
    entity_type.batch_create_features(feature_configs=feature_configs)
except Exception as e:
    print(f"Features may already exist: {e}")


# Test Function: write and delete a dummy entity
def test_featurestore_service(entity_type):
    test_entity_id = "test_entity_001"

    feature_values = {}
    for _, (fid, ftype) in FEATURES_SCHEMA.items(): # force feature values to types
        if ftype == "DOUBLE":
            feature_values[fid] = 0.0
        else:
            feature_values[fid] = 0

    entity_type.write_feature_values(
        {test_entity_id: feature_values}  # <- must be keyed by entity_id
    )
    retrieved = entity_type.read(entity_ids=[test_entity_id])
    for feature_name, feature_value in retrieved.iloc[0].items():
        if feature_name != "entity_id":
            print(feature_name, feature_value)    
        

if __name__ == "__main__":
    test_featurestore_service(entity_type)
