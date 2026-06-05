from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import os
import sys
import subprocess

from app.models import FinancialInstrument, TimeSeriesPoint
from app.database import (
    insert_or_update_instrument,
    delete_instrument_marker,
    get_instrument_as_of,
    instruments_collection,
    timeseries_collection,
    vendors_collection,
    results_collection 
)
from ingestion.fetcher import process_and_ingest

app = FastAPI(
    title="Acme Ltd Financial Data Warehouse",
    description="Temporal Bi-Temporal NoSQL Data Warehouse exposing financial assets and machine learning metrics via REST APIs."
)

@app.get("/api/assets")
async def get_all_assets_summary():
    pipeline = [
        {"$sort": {"System_Timestamp": -1}},
        {
            "$group": {
                "_id": "$Instrument_ID",
                "Symbol": {"$first": "$Symbol"},
                "Class": {"$first": "$Class"},
                "status": {"$first": "$status"}
            }
        },
        {"$match": {"status": "active"}}
    ]
    cursor = instruments_collection.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    return [
        {
            "assetId": item["_id"],
            "symbol": item["Symbol"],
            "class": item["Class"]
        } for item in results
    ]

@app.get("/api/assets/{instrument_id}")
async def get_asset_details(
    instrument_id: str, 
    as_of: Optional[datetime] = Query(None, description="ISO Timestamp to view historical data state (Temporal Query)")
):
    target_time = as_of if as_of else datetime.now(timezone.utc)
    record = await get_instrument_as_of(instrument_id, target_time)
    
    if not record:
        raise HTTPException(status_code=404, detail="Financial Instrument not found in the system history.")
    
    if "message" in record:
        raise HTTPException(status_code=410, detail=record["message"])
        
    record.pop("_id", None)
    return record

@app.get("/api/timeseries/{instrument_id}/{vendor_id}")
async def get_time_series_data(
    instrument_id: str,
    vendor_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    query = {
        "Instrument_ID": instrument_id.upper(),
        "Vendor_ID": vendor_id.upper()
    }
    
    if start_date or end_date:
        query["Timestamp"] = {}
        if start_date:
            query["Timestamp"]["$gte"] = start_date
        if end_date:
            query["Timestamp"]["$lte"] = end_date

    cursor = timeseries_collection.find(query).sort("Timestamp", 1)
    points = await cursor.to_list(length=1000)
    
    cleaned_results = []
    for p in points:
        if "_id" in p:
            p["_id"] = str(p["_id"])
        for time_key in ["Timestamp", "Valid_From", "System_Timestamp"]:
            if isinstance(p.get(time_key), datetime):
                p[time_key] = p[time_key].isoformat()
        cleaned_results.append(p)
        
    return cleaned_results

@app.get("/api/analytics/summary/{instrument_id}")
async def get_asset_analytics_summary(instrument_id: str):
    cursor = results_collection.find({"Instrument_ID": instrument_id.upper()}).sort("Timestamp", -1).limit(1)
    batch_results = await cursor.to_list(length=1)
    
    if batch_results:
        record = batch_results[0]
        return {
            "instrument_id": record.get("Instrument_ID"),
            "average_close_price": record.get("Rolling_Avg_Close", 0.0),
            "max_close_price": record.get("Historical_Max", 0.0),
            "min_close_price": record.get("Historical_Min", 0.0),
            "volatility": record.get("Asset_Volatility", 0.0),
            "spark_ml_prediction": record.get("Spark_ML_Prediction", 0.0),
            "source_engine": "Native Native Pipeline Engine Batch Engine [Optimized]"
        }

    pipeline = [
        {"$match": {"Instrument_ID": instrument_id.upper()}},
        {
            "$group": {
                "_id": "$Instrument_ID",
                "average_close_price": {"$avg": "$indicators.Close"},
                "max_close_price": {"$max": "$indicators.Close"},
                "min_close_price": {"$min": "$indicators.Close"}
            }
        }
    ]
    cursor = timeseries_collection.aggregate(pipeline)
    raw_results = await cursor.to_list(length=1)
    
    if not raw_results:
        return {
            "average_close_price": 0.0,
            "max_close_price": 0.0,
            "min_close_price": 0.0,
            "source_engine": "Fallback Static Core Aggregator"
        }
        
    return raw_results[0]

@app.post("/api/assets")
async def create_or_version_asset(instrument: FinancialInstrument):
    return await insert_or_update_instrument(instrument)

@app.delete("/api/assets/{instrument_id}")
async def soft_delete_asset(instrument_id: str, valid_from: Optional[datetime] = None):
    effective_date = valid_from if valid_from else datetime.now(timezone.utc)
    return await delete_instrument_marker(instrument_id, effective_date)

@app.post("/api/ingest/trigger")
async def trigger_data_ingestion(
    symbol: str, 
    instrument_id: str, 
    background_tasks: BackgroundTasks, 
    vendor_id: str = "VEND_NASDAQ"
):
    instrument = await instruments_collection.find_one({"Instrument_ID": instrument_id.upper()})
    if not instrument:
        raise HTTPException(
            status_code=400, 
            detail=f"Instrument ID '{instrument_id}' must be configured before streaming history."
        )
    
    background_tasks.add_task(process_and_ingest, symbol.upper(), instrument_id.upper(), vendor_id.upper())
    return {
        "status": "accepted", 
        "message": f"Ingestion pipeline successfully triggered for {symbol}. Processing in background..."
    }

@app.get("/api/sources")
async def get_all_data_sources():
    cursor = vendors_collection.find({}, {"_id": 0, "Vendor_ID": 1, "Vendor_Name": 1})
    results = await cursor.to_list(length=100)
    return [{"dataSourceId": r["Vendor_ID"], "name": r["Vendor_Name"]} for r in results]

@app.get("/api/sources/{vendor_id}")
async def get_source_details(vendor_id: str):
    vendor = await vendors_collection.find_one({"Vendor_ID": vendor_id.upper()}, {"_id": 0})
    if not vendor:
        raise HTTPException(status_code=404, detail="Data source provider not found.")
    return vendor

@app.post("/api/analytics/spark/trigger")
async def trigger_spark_analytics_pipeline(background_tasks: BackgroundTasks):
    def run_script():
        try:
            subprocess.run([sys.executable, "server/spark_analytics.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Spark process execution failed: {e}")

    background_tasks.add_task(run_script)
    return {
        "status": "accepted",
        "message": "Apache Spark analytical cluster engine triggered successfully in the background."
    }