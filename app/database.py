import os
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
from app.models import FinancialInstrument, TimeSeriesPoint
from dotenv import load_dotenv

# FIX 1: Dynamically compute absolute project root to find your cloud .env file
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
load_dotenv(dotenv_path=os.path.join(project_root, ".env"))

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("CRITICAL: 'MONGO_URI' is empty inside database.py context. Verify root level .env!")

client = AsyncIOMotorClient(MONGO_URI)
db = client["acme_dwh"]

# Collection Mappings
instruments_collection = db["financial_instruments"]
timeseries_collection = db["time_series"]
vendors_collection = db["vendors"]
results_collection = db["spark_analytics_results"]

async def insert_or_update_instrument(instrument: FinancialInstrument):
    doc = instrument.dict(by_alias=True)
    doc["System_Timestamp"] = datetime.now(timezone.utc)
    await instruments_collection.insert_one(doc)
    return {"status": "success", "message": "Version successfully appended."}

async def delete_instrument_marker(instrument_id: str, valid_from: datetime):
    last_version = await instruments_collection.find_one(
        {"Instrument_ID": instrument_id},
        sort=[("System_Timestamp", -1)]
    )
    
    if not last_version:
        return {"status": "error", "message": "Instrument not found."}
    
    tombstone = {
        "Instrument_ID": instrument_id,
        "Symbol": last_version["Symbol"],
        "Class": last_version["Class"],
        "Region_ID": last_version["Region_ID"],
        "Description": last_version["Description"],
        "System_Timestamp": datetime.now(timezone.utc),
        "status": "inactive",  
        "Valid_From": valid_from
    }
    
    await instruments_collection.insert_one(tombstone)
    return {"status": "deleted", "message": f"Asset marked inactive starting {valid_from}"}

async def get_instrument_as_of(instrument_id: str, point_in_time: datetime):
    cursor = instruments_collection.find(
        {
            "Instrument_ID": instrument_id,
            "System_Timestamp": {"$lte": point_in_time}
        }
    ).sort("System_Timestamp", -1).limit(1)
    
    results = await cursor.to_list(length=1)
    
    if not results:
        return None
        
    record = results[0]
    
    if record.get("status") == "inactive":
        return {"message": "Asset was inactive or deleted at this point in time."}
        
    return record

async def get_analytics_summary(instrument_id: str):
    cursor = results_collection.find({"Instrument_ID": instrument_id.upper()}).sort("Timestamp", -1).limit(1)
    results = await cursor.to_list(length=1)
    if not results:
        return None
        
    record = results[0]
    return {
        "instrument_id": record.get("Instrument_ID"),
        "average_close_price": record.get("Rolling_Avg_Close"),
        "max_close_price": record.get("Historical_Max"),
        "min_close_price": record.get("Historical_Min"),
        "volatility": record.get("Asset_Volatility"),
        "spark_ml_prediction": record.get("Spark_ML_Prediction")
    }