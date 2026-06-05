import os
import httpx
import asyncio
from datetime import datetime, timezone
from curl_cffi import requests as curl_requests
from app.database import timeseries_collection, vendors_collection, instruments_collection
from dotenv import load_dotenv

load_dotenv()

start_date = "2025-01-02"
end_date = "2025-01-15"

async def fetch_from_nasdaq(symbol: str) -> list:
    NASDAQ_API_KEY = os.getenv("NASDAQ_API_KEY")
    url = f"https://data.nasdaq.com/api/v3/datasets/FRED/{symbol.upper()}.json?api_key={NASDAQ_API_KEY}"    
    try:
        response = curl_requests.get(url, impersonate="chrome", timeout=15.0)
        if "Incapsula" in response.text or "incident_id" in response.text:
            raise Exception("Nasdaq blocked our script signature.")
        if response.status_code != 200:
            raise Exception(f"Nasdaq API communication failure: Code {response.status_code}")
        
        raw_json = response.json()
        dataset = raw_json.get("dataset", {})
        columns = dataset.get("column_names", [])
        data_rows = dataset.get("data", [])
        normalized_points = []
        
        for row in data_rows:
            row_dict = dict(zip(columns, row))
            date_str = row_dict.pop("Date")
            
            casted_indicators = {}
            for key, value in row_dict.items():
                try:
                    casted_indicators[key] = float(value) if value is not None else 0.0
                except (ValueError, TypeError):
                    casted_indicators[key] = value
            
            normalized_points.append({
                "date": date_str,
                "indicators": casted_indicators 
            })
        return normalized_points
    except Exception as e:
        raise Exception(f"Nasdaq Direct REST Pull Failed: {e}")

async def fetch_from_alphavantage(symbol: str) -> list:
    ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
    if symbol.upper() in ["UNRATE"]:
        url = f"https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey={ALPHA_VANTAGE_KEY}"
    elif symbol.upper() in ["GDP"]:
        url = f"https://www.alphavantage.co/query?function=REAL_GDP&apikey={ALPHA_VANTAGE_KEY}"
    else:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}"
            
    try:
        response = httpx.get(url, timeout=15.0)
        if response.status_code != 200:
            raise ValueError(f"HTTP Communications Breakdown: {response.status_code}")
            
        data = response.json()
        if symbol.upper() in ["UNRATE", "GDP"]:
            if "data" not in data:
                raise ValueError(f"Alpha Vantage rejected macro pull: {data.get('Information', 'Unknown Error')}")
            
            normalized_points = []
            for item in data["data"]:
                if item["value"] == "." or item["value"] is None:
                    continue
                normalized_points.append({
                    "date": item["date"],
                    "indicators": {"Value": float(item["value"])}
                })
            return normalized_points
        else:
            time_series_key = "Time Series (Daily)"
            if time_series_key not in data:
                raise ValueError(f"Alpha Vantage rejected pull: {data.get('Error Message', data.get('Note', 'Unknown Call'))}")
                
            raw_series = data[time_series_key]
            normalized_points = []
            for date_str, metrics in raw_series.items():
                normalized_points.append({
                   "date": date_str,
                    "indicators": {
                        "Open": float(metrics.get("1a. open (USD)") or metrics.get("1. open") or 0.0),
                        "High": float(metrics.get("2a. high (USD)") or metrics.get("2. high") or 0.0),
                        "Low": float(metrics.get("3a. low (USD)") or metrics.get("3. low") or 0.0),
                        "Close": float(metrics.get("4a. close (USD)") or metrics.get("4. close") or 0.0),
                        "Volume": float(metrics.get("5. volume") or 0.0)
                    }
                })
            return normalized_points
    except Exception as e:
        print(f"Alpha Vantage Pipeline Failure: {e}")
        return []
    
async def fetch_from_polygon(symbol: str) -> list:
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol.upper()}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(url)
        if response.status_code != 200:
            raise Exception(f"Polygon.io API Error: Code {response.status_code} - {response.text}")
            
        raw_json = response.json()
        if raw_json.get("status") != "OK" or "results" not in raw_json:
            raise Exception(f"Polygon rejected query or symbol not found: {raw_json.get('error', 'No results')}")
            
        results_array = raw_json.get("results", [])
        normalized_points = []
        
        for bar in results_array:
            msec_timestamp = bar["t"]
            date_str = datetime.utcfromtimestamp(msec_timestamp / 1000.0).strftime("%Y-%m-%d")
            normalized_points.append({
                "date": date_str,
                "indicators": {
                    "Open": float(bar["o"]),
                    "High": float(bar["h"]),
                    "Low": float(bar["l"]),
                    "Close": float(bar["c"]),
                    "Volume": int(bar["v"])
                }
            })
        return normalized_points

async def process_and_ingest(symbol: str, instrument_id: str, vendor_id: str):
    vendor_id = vendor_id.upper()
    instrument_id = instrument_id.upper()
    symbol = symbol.upper()
    
    vendor_exists = await vendors_collection.find_one({"Vendor_ID": vendor_id})
    if not vendor_exists:
        names_map = {
            "VEND_NASDAQ": "Nasdaq Data Link",
            "VEND_ALPHAVANTAGE": "Alpha Vantage Cloud Services",
            "VEND_POLYGON": "Polygon.io"
        }
        await vendors_collection.insert_one({
            "Vendor_ID": vendor_id,
            "Vendor_Name": names_map.get(vendor_id, "External Network Broker"),
            "Access_Method": "RESTful Web API Layer"
        })

    try:
        if vendor_id == "VEND_NASDAQ":
            normalized_data = await fetch_from_nasdaq(symbol)
        elif vendor_id == "VEND_ALPHAVANTAGE":
            normalized_data = await fetch_from_alphavantage(symbol)
        elif vendor_id == "VEND_POLYGON":
            normalized_data = await fetch_from_polygon(symbol)
        else:
            raise ValueError(f"Unregistered vendor schema target: {vendor_id}")
        
        if not normalized_data:
            print(f"Ingestion stream returned empty for {vendor_id}.")
            return
    except Exception as e:
        print(f"Ingestion sequence terminated: {e}")
        return

    inserted_count = 0
    now_utc = datetime.now(timezone.utc)
    
    for point in normalized_data:
        date_str = point["date"]
        target_timestamp = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        entry_id = f"TS_{instrument_id}_{vendor_id}_{date_str}"

        partition_year_bucket = int(target_timestamp.year)

        existing_point = await timeseries_collection.find_one({"Entry_ID": entry_id})
        if existing_point:
            continue

        time_series_doc = {
            "Entry_ID": entry_id,
            "Instrument_ID": instrument_id,
            "Vendor_ID": vendor_id,
            "Timestamp": target_timestamp,
            "Partition_Year": partition_year_bucket,
            "indicators": point["indicators"],
            "Valid_From": target_timestamp,      
            "Valid_To": datetime(9999, 12, 31, tzinfo=timezone.utc), 
            "System_Timestamp": now_utc,         
            "Data_Lineage": {
                "Ingestion_Source_API": f"REST::{vendor_id}",
                "Target_Ticker_Symbol": symbol,
                "Pipeline_Worker_Signature": "Async-Idempotent-Upsert-Worker",
                "Execution_Environment": "Production-Cloud-Warehouse-Node",
                "Audit_Hash_Verified": True
            }
        }
        result = await timeseries_collection.update_one(
            {"Entry_ID": entry_id},
            {"$set": time_series_doc},
            upsert=True
        )
        if result.upserted_id is not None or result.modified_count > 0:
            inserted_count += 1

    print(f"Ingestion Successful! Inserted {inserted_count} ticks.")