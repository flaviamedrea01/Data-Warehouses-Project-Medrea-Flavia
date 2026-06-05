import os
from typing import Dict, Any
from dotenv import load_dotenv
from pymongo import MongoClient

current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
load_dotenv(dotenv_path=os.path.join(project_root, ".env"))

mongo_atlas_uri = os.getenv("MONGO_URI")
if not mongo_atlas_uri:
    raise RuntimeError("'MONGO_URI' not found in environment variables. Verify your .env file.")

client = MongoClient(mongo_atlas_uri)
db = client["acme_dwh"]
instruments_collection = db["financial_instruments"]
timeseries_collection = db["time_series"]
results_collection = db["spark_analytics_results"]

async def list_assets_tool() -> str:
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
    results = list(instruments_collection.aggregate(pipeline))
    return str([{"assetId": r["_id"], "Symbol": r["Symbol"], "Class": r["Class"]} for r in results])

async def fetch_time_series_tool(instrument_id: str, vendor_id: str) -> str:
    cursor = timeseries_collection.find(
        {"Instrument_ID": instrument_id.upper(), "Vendor_ID": vendor_id.upper()},
        {"_id": 0}
    ).sort("Timestamp", -1).limit(50)
    
    points = list(cursor)
    for p in points:
        if isinstance(p.get("Timestamp"), lambda x: hasattr(x, "isoformat")):
            p["Timestamp"] = p["Timestamp"].isoformat()
    return str(points)

async def summarize_trends_tool(instrument_id: str) -> str:
    cursor = results_collection.find({"Instrument_ID": instrument_id.upper()}).sort("Timestamp", -1).limit(1)
    results = list(cursor)
    if not results:
        return str({"message": "No metrics compiled yet."})
    record = results[0]
    return str({
        "instrument_id": record.get("Instrument_ID"),
        "average_close_price": record.get("Rolling_Avg_Close"),
        "max_close_price": record.get("Historical_Max"),
        "min_close_price": record.get("Historical_Min")
    })

async def compare_assets_tool(asset_a: str, asset_b: str) -> str:
    res_a = results_collection.find_one({"Instrument_ID": asset_a.upper()}, sort=[("Timestamp", -1)])
    res_b = results_collection.find_one({"Instrument_ID": asset_b.upper()}, sort=[("Timestamp", -1)])
    return str({
        asset_a: {"Rolling_Avg": res_a.get("Rolling_Avg_Close") if res_a else "N/A"},
        asset_b: {"Rolling_Avg": res_b.get("Rolling_Avg_Close") if res_b else "N/A"}
    })

async def explain_variance_delta_tool(instrument_id: str, vendor_id: str) -> str:
    res = results_collection.find_one({"Instrument_ID": instrument_id.upper()}, sort=[("Timestamp", -1)])
    return str({
        "asset": instrument_id,
        "recent_volatility_index": res.get("Asset_Volatility", 0.0) if res else 0.0
    })

async def predict_future_trends_tool(instrument_id: str) -> str:
    try:
        cursor = results_collection.find({"Instrument_ID": instrument_id.upper()}).sort([("Timestamp", -1)]).limit(1)
        results = list(cursor)
        
        if not results:
            return str({"error": f"No analytics execution prediction records found for asset {instrument_id}. Please run the Spark batch pipeline on the sidebar first."})
            
        record = results[0]
        return str({
            "asset": instrument_id,
            "engine": "Analytical Native Pipeline Batch Engine",
            "last_actual_value": record.get("Actual_Value"),
            "spark_computed_moving_average": round(record.get("Rolling_Avg_Close", 0), 2),
            "spark_computed_volatility_index": round(record.get("Asset_Volatility", 0), 4),
            "spark_ml_next_period_prediction": round(record.get("Spark_ML_Prediction", 0), 2)
        })
    except Exception as e:
        return str({"error": f"Failed executing cloud collection lookup matrix: {str(e)}"})

MCP_TOOLS_MANIFEST = {
    "list_assets": list_assets_tool,
    "fetch_time_series": fetch_time_series_tool,
    "summarize_trends": summarize_trends_tool,
    "compare_assets": compare_assets_tool,
    "explain_variance_delta": explain_variance_delta_tool,
    "predict_future_trends": predict_future_trends_tool,
}

async def execute_mcp_tool(tool_name: str, arguments: Dict[str, Any]) -> str:
    if tool_name in MCP_TOOLS_MANIFEST:
        tool_func = MCP_TOOLS_MANIFEST[tool_name]
        try:
            if tool_name == "list_assets":
                return await tool_func()
            elif tool_name in ["summarize_trends", "predict_future_trends"]:
                return await tool_func(arguments["instrument_id"])
            elif tool_name == "fetch_time_series":
                return await tool_func(arguments["instrument_id"], arguments["vendor_id"])
            elif tool_name == "compare_assets":
                return await tool_func(arguments["asset_a"], arguments["asset_b"])
            elif tool_name == "explain_variance_delta":
                return await tool_func(arguments["instrument_id"], arguments["vendor_id"])
        except Exception as e:
            return str({"error": f"Internal mapping crash: {str(e)}"})
    return str({"error": "Unknown tool configuration requested."})