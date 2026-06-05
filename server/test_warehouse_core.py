import os
import sys
import pytest
import asyncio
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
load_dotenv(dotenv_path=os.path.join(project_root, ".env"))

MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["acme_dwh"]

instruments_col = db["financial_instruments"]
timeseries_col = db["time_series"]

@pytest.mark.asyncio
async def test_dal_lifecycle_save_find_latest_find_all():
    test_id = "TEST_DAL_RUBRIC_VECTOR"
    
    instruments_col.delete_many({"Instrument_ID": test_id})
    
    v1_doc = {
        "Instrument_ID": test_id,
        "Symbol": "RUBRIC",
        "Class": "Fixed_Income",
        "System_Timestamp": datetime.now(timezone.utc),
        "status": "active",
        "Description": "Version 1 Initial Entry"
    }
    instruments_col.insert_one(v1_doc)
    
    v2_doc = {
        "Instrument_ID": test_id,
        "Symbol": "RUBRIC",
        "Class": "Fixed_Income",
        "System_Timestamp": datetime.now(timezone.utc),
        "status": "active",
        "Description": "Version 2 Overwrite Entry"
    }
    instruments_col.insert_one(v2_doc)
    
    latest_record = instruments_col.find_one(
        {"Instrument_ID": test_id}, 
        sort=[("System_Timestamp", -1)]
    )
    assert latest_record is not None, "DAL Fail: Unable to locate saved asset structural matrix."
    assert latest_record["Description"] == "Version 2 Overwrite Entry", "DAL Fail: findLatest did not return the newest system state version."
    
    all_historical_versions = list(instruments_col.find({"Instrument_ID": test_id}))
    assert len(all_historical_versions) == 2, f"DAL Fail: findAll should hold 2 historical timeline blocks, found {len(all_historical_versions)}"
    
    instruments_col.delete_many({"Instrument_ID": test_id})

@pytest.mark.asyncio
async def test_ingestion_pipeline_idempotency_and_lineage():
    mock_entry_id = "TS_MOCK_INGEST_ID_999"
    timeseries_col.delete_many({"Entry_ID": mock_entry_id})
    
    payload = {
        "Entry_ID": mock_entry_id,
        "Instrument_ID": "TEST_ASSET",
        "Vendor_ID": "TEST_VEND",
        "Timestamp": datetime.now(timezone.utc),
        "Partition_Year": int(datetime.now(timezone.utc).year), 
        "indicators": {"Close": 150.0},
        "Data_Lineage": {
            "Ingestion_Source_API": "REST::TEST_MOCK",
            "Pipeline_Worker_Signature": "Automated-QA-Verification-Engine",
            "Audit_Hash_Verified": True
        }
    }
    
    timeseries_col.update_one({"Entry_ID": mock_entry_id}, {"$set": payload}, upsert=True)
    
    timeseries_col.update_one({"Entry_ID": mock_entry_id}, {"$set": payload}, upsert=True)
    
    records = list(timeseries_col.find({"Entry_ID": mock_entry_id}))
    assert len(records) == 1, "Idempotency Safety Failure: Concurrent ingestion requests generated duplicate entry records!"
    assert "Data_Lineage" in records[0], "Data Lineage verification failed: Missing mandatory origin tracking audit nodes."
    
    timeseries_col.delete_many({"Entry_ID": mock_entry_id})