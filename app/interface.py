import streamlit as st
import httpx
import plotly.graph_objects as go
from datetime import datetime
import asyncio
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
server_dir = os.path.join(project_root, "server")
if server_dir not in sys.path:
    sys.path.append(server_dir)

import mcp_server


st.set_page_config(page_title="Acme Ltd Data Warehouse", layout="wide")

API_BASE_URL = "http://127.0.0.1:8000/api"

st.title("Acme Ltd - Financial Data Warehouse Dashboard")
st.markdown("---")

st.sidebar.header("Asset Registration")

with st.sidebar.form("register_asset_form"):
    new_id = st.text_input("Instrument ID", value="INST_AAPL_001")
    new_symbol = st.text_input("Symbol", value="AAPL")
    new_class = st.selectbox("Asset Class", ["stock", "bond", "crypto", "options", "futures"])
    new_region = st.text_input("Region of Origin (Region_ID)", value="REG_US")
    new_desc = st.text_area("Asset Description", value="Apple Inc. Common Stock")
    
    submit_register = st.form_submit_button("Save Asset Metadata")
    
    if submit_register:
        try:
            asset_payload = {
                "Instrument_ID": new_id,
                "Symbol": new_symbol,
                "Class": new_class,
                "Region_ID": new_region,
                "Description": new_desc,
                "additional_attributes": {}
            }
            reg_resp = httpx.post(f"{API_BASE_URL}/assets", json=asset_payload)
            if reg_resp.status_code == 200:
                st.success(f"Asset metadata container '{new_id}' successfully written to temporal storage!")
            else:
                st.error(f"Error: {reg_resp.text}")
        except Exception as e:
            st.error(f"Connection error: {e}")

st.sidebar.markdown("---")
st.sidebar.header("Ingestion Engine")

with st.sidebar.form("ingest_form"):
    asset_symbol = st.text_input("Ticker Symbol / Code", value="AAPL")
    asset_id = st.text_input("Instrument ID", value="INST_AAPL_001")
    
    chosen_vendor = st.selectbox(
        "Choose Financial Data Provider",
        options=["VEND_ALPHAVANTAGE", "VEND_NASDAQ", "VEND_POLYGON"],
        format_func=lambda x: {
            "VEND_ALPHAVANTAGE": "Alpha Vantage Marketplace API",
            "VEND_NASDAQ": "Nasdaq Data Link Feed",
            "VEND_POLYGON": "Polygon.io"
        }.get(x)
    )
    
    submit_ingest = st.form_submit_button("Trigger Ingestion")
    
    if submit_ingest:
        try:
            with st.spinner("Fetching data from provider..."):
                ingest_resp = httpx.post(
                    f"{API_BASE_URL}/ingest/trigger", 
                    params={
                        "symbol": asset_symbol, 
                        "instrument_id": asset_id,
                        "vendor_id": chosen_vendor
                    },
                    timeout=30.0
                )
                if ingest_resp.status_code == 200:
                    st.success(ingest_resp.json().get("message", "Ingestion completed successfully!"))
                else:
                    st.error(f"Error: {ingest_resp.json().get('detail', 'Unknown error occurrence.')}")
        except Exception as e:
            st.error(f"Could not connect to backend API: {e}")


tab1, tab2, tab3 = st.tabs(["Asset Discovery & Big Data", "Market Charts & Analytics", "AI Assistant Tooling"])

with tab1:
    st.header("Financial Asset Discovery & Big Data Layout")
    
    try:
        assets_resp = httpx.get(f"{API_BASE_URL}/assets")
        assets = assets_resp.json()
        
        if not assets:
            st.info("No active assets found. Use the sidebar to register and ingest metadata.")
        else:
            st.subheader("Available Active Assets Summary")
            st.dataframe(assets, width='stretch')
            
            st.markdown("---")
            
            st.subheader("Temporal Point-in-Time Record Inspector")
            
            raw_options = [a.get("Instrument_ID", a.get("assetId", "Unknown")) for a in assets]
            asset_options = sorted(list(set(raw_options)))
            
            if "saved_asset" not in st.session_state:
                st.session_state.saved_asset = asset_options[0] if asset_options else "Unknown"
            try:
                default_idx = asset_options.index(st.session_state.saved_asset)
            except ValueError:
                default_idx = 0

            selected_asset = st.selectbox("Select Asset to Inspect", asset_options, index=default_idx)
            st.session_state.saved_asset = selected_asset
            
            use_history = st.checkbox("View Historical Point-in-Time State (Temporal Query)")
            
            if use_history:
                if "saved_date" not in st.session_state:
                    st.session_state.saved_date = datetime.now().date()
                if "saved_time" not in st.session_state:
                    st.session_state.saved_time = datetime.now().time()
                
                as_of_date = st.date_input("Select Historical State Date", key="saved_date")
                as_of_time = st.time_input("Select Historical State Time", key="saved_time")
            else:
                as_of_date = datetime.now().date()
                as_of_time = datetime.now().time()
                
            if st.button("Inspect Asset State"):
                as_of_datetime = datetime.combine(as_of_date, as_of_time) if use_history else datetime.now()
                params = {"as_of": as_of_datetime.isoformat()} if use_history else {}
                
                with st.spinner(f"Querying temporal ledger for {selected_asset}..."):
                    detail_resp = httpx.get(f"{API_BASE_URL}/assets/{selected_asset}", params=params)
                    
                    if detail_resp.status_code == 200:
                        st.write(f"### Audit Logs Snapshot: `{selected_asset}`")
                        st.json(detail_resp.json())
                    elif detail_resp.status_code == 410:
                        st.warning(f"⚠️ {detail_resp.json().get('detail', 'Record no longer valid.')}")
                    else:
                        st.error("Asset not found in system logs at this specified time marker.")
            
            st.markdown("---")
            
            st.subheader("Apache Spark Big-Data Analytics Control Console")
            st.caption("Triggers the PySpark distributed batch processing engine to calculate rolling volatility metrics and fit the Spark ML Linear Regression model across warehouse collections.")

            if st.button("Run Spark Aggregation & ML Pipeline"):
                with st.spinner("Running Spark Job... (Loading distributed JVM contexts)"):
                    try:
                        spark_resp = httpx.post(f"{API_BASE_URL}/analytics/spark/trigger", timeout=60.0)
                        if spark_resp.status_code == 200:
                            st.success("Spark Analytics & ML Pipeline Completed Successfully!")
                            st.json(spark_resp.json())
                        else:
                            st.error(f"Spark Job Framework Return Error: {spark_resp.text}")
                    except Exception as e:
                        st.error(f"Could not reach Spark Backend Router: {e}")
                        
    except Exception as e:
        st.error(f"Backend REST Server Offline: {e}")

with tab2:
    st.header("Time-Series & Analytical Insights")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        with st.form("analytics_chart_form"):
            target_asset = st.text_input("Enter Instrument ID for Charts", value="INST_AAPL_001")
            target_vendor = st.text_input("Enter Data Source / Vendor ID", value="VEND_ALPHAVANTAGE")
            
            view_charts = st.form_submit_button("Load Historical Trends")
        
    with col2:
        if view_charts:
            try:
                analytics_resp = httpx.get(f"{API_BASE_URL}/analytics/summary/{target_asset}")
                if analytics_resp.status_code == 200:
                    metrics = analytics_resp.json()
                    avg_val = metrics.get('average_close_price') or metrics.get('average_value') or 0.0
                    max_val = metrics.get('max_close_price') or metrics.get('max_value') or 0.0
                    min_val = metrics.get('min_close_price') or metrics.get('min_value') or 0.0
                    
                    m_col1, m_col2, m_col3 = st.columns(3)
                    
                    if "UNRATE" in target_asset.upper() or "GDP" in target_asset.upper():
                        is_unrate = "UNRATE" in target_asset.upper()
                        m_col1.metric("Average Indicator Value", f"{avg_val:.2f}%" if is_unrate else f"${avg_val:,.2f}")
                        m_col2.metric("Highest Value", f"{max_val:.2f}%" if is_unrate else f"${max_val:,.2f}")
                        m_col3.metric("Lowest Value", f"{min_val:.2f}%" if is_unrate else f"${min_val:,.2f}")
                    else:
                        m_col1.metric("Average Close Price", f"${avg_val:.2f}")
                        m_col2.metric("Highest Close Price", f"${max_val:.2f}")
                        m_col3.metric("Lowest Close Price", f"${min_val:.2f}")
                else:
                    st.warning("Could not gather analytical metrics summary for this entity code.")
                
                ts_resp = httpx.get(f"{API_BASE_URL}/timeseries/{target_asset}/{target_vendor}")
                if ts_resp.status_code == 200 and ts_resp.json():
                    data_points = ts_resp.json()
                    
                    dates = [dp["Timestamp"][:10] for dp in data_points]
                    closes = [dp["indicators"].get("Close") or dp["indicators"].get("Value") or 0 for dp in data_points]
                    opens = [dp["indicators"].get("Open") or dp["indicators"].get("Value") or 0 for dp in data_points]
                    
                    fig = go.Figure()
                    y_axis_label = "Indicator Value" if ("UNRATE" in target_asset.upper() or "GDP" in target_asset.upper()) else "Price (USD)"
                    trace_name = "Value" if ("UNRATE" in target_asset.upper() or "GDP" in target_asset.upper()) else "Close Price"
                    
                    fig.add_trace(go.Scatter(x=dates, y=closes, mode='lines+markers', name=trace_name, line=dict(color='#00ffcc')))
                    
                    if "UNRATE" not in target_asset.upper() and "GDP" not in target_asset.upper():
                        fig.add_trace(go.Scatter(x=dates, y=opens, mode='lines', name='Open Price', line=dict(dash='dash')))
                        
                    fig.update_layout(
                        title=f"Historical Price Action Matrix ({target_asset})", 
                        template="plotly_dark", 
                        xaxis_title="Date", 
                        yaxis_title=y_axis_label
                    )
                
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.info("No time series data records returned for this asset/vendor combination.")
            except Exception as e:
                st.error(f"Error gathering statistical profiles: {e}")


with tab3:
    st.header("Natural Language Warehouse Assistant (MCP Interface)")

    if "mcp_asset_context" not in st.session_state:
        st.session_state["mcp_asset_context"] = "INST_AAPL_001"
    if "mcp_vendor_context" not in st.session_state:
        st.session_state["mcp_vendor_context"] = "VEND_ALPHAVANTAGE"

    st.markdown("""
    **Try these supported live warehouse commands:**
    * `list assets`
    * `fetch time series for INST_AAPL_001 from VEND_ALPHAVANTAGE`
    * `summarize trends for INST_AAPL_001`
    * `compare two assets: INST_AAPL_001 and INST_BTC_001`
    * `explain a change for INST_AAPL_001`
    * `predict future trends for INST_AAPL_001`
    """)

    user_prompt = st.text_input(
        "Ask the Assistant about warehouse data:", 
        placeholder="e.g., compare two assets: INST_AAPL_001 and INST_BTC_001",
        key="mcp_text_input"
    )
    
    if st.button("Ask Assistant", key="mcp_trigger_button"):
        if user_prompt:
            with st.spinner("LLM Planner extracting entities and mapping protocol tools..."):
                prompt_lower = user_prompt.lower()
                words = [w.strip("?,.!\"'()[]:;").upper() for w in user_prompt.split()]
                
                try:
                    active_assets = httpx.get(f"{API_BASE_URL}/assets", timeout=3.0).json()
                    known_instruments = [a.get("Instrument_ID", a.get("assetId", "")).upper() for a in active_assets]
                except Exception:
                    known_instruments = ["INST_AAPL_001", "INST_BTC_001", "INST_UNRATE_US", "INST_MSFT_001"]
                
                found_assets = [asset for asset in known_instruments if asset in words or asset.lower() in prompt_lower]
                is_general_command = "list assets" in prompt_lower or "show assets" in prompt_lower

                if not is_general_command and not found_assets:
                    st.warning("**Asset Context Missing:** Please specify a valid Instrument ID (e.g., `INST_AAPL_001`) in your prompt so the assistant knows which asset to analyze.")
                    st.stop()

                if found_assets:
                    st.session_state["mcp_asset_context"] = found_assets[0]
                
                for word in words:
                    if "VEND_" in word:
                        st.session_state["mcp_vendor_context"] = word
                
                current_target_asset = st.session_state["mcp_asset_context"]
                current_target_vendor = st.session_state["mcp_vendor_context"]

                tool_name = None
                arguments = {}

                if is_general_command:
                    tool_name = "list_assets"
                    arguments = {}
                    
                elif "time series" in prompt_lower or "fetch time" in prompt_lower:
                    tool_name = "fetch_time_series"
                    arguments = {"instrument_id": current_target_asset, "vendor_id": current_target_vendor}
                 
                elif "predict" in prompt_lower or "forecast" in prompt_lower:
                    tool_name = "predict_future_trends"
                    arguments = {"instrument_id": current_target_asset}   
                    
                elif "trends" in prompt_lower or "summarize" in prompt_lower:
                    tool_name = "summarize_trends"
                    arguments = {"instrument_id": current_target_asset}
                    
                elif "compare" in prompt_lower or " vs " in prompt_lower or "versus" in prompt_lower:
                    tool_name = "compare_assets"
                    if len(found_assets) >= 2:
                        arguments = {"asset_a": found_assets[0], "asset_b": found_assets[1]}
                    else:
                        st.warning("**Comparison Error:** Please provide two Instrument IDs to compare (e.g., `compare INST_AAPL_001 and INST_BTC_001`).")
                        st.stop()
                        
                elif "explain" in prompt_lower or "change" in prompt_lower:
                    tool_name = "explain_variance_delta"
                    arguments = {"instrument_id": current_target_asset, "vendor_id": current_target_vendor}

                if tool_name:
                    st.info(f"**MCP Protocol Mapping Identified:** `{tool_name}`")
                    st.code(f"Dispatched Tool Arguments: {arguments}", language="json")
                    
                    try:
                        raw_response_string = asyncio.run(mcp_server.execute_mcp_tool(tool_name, arguments))
                        st.write("Grounded AI Response")
                        st.caption("Factual JSON data structure returned from the isolated storage engine:")
                        
                        try:
                            import ast
                            python_object = ast.literal_eval(raw_response_string)
                            if isinstance(python_object, list):
                                st.dataframe(python_object, width='stretch')
                            else:
                                st.json(python_object)
                        except Exception:
                            st.text(raw_response_string)
                            
                    except Exception as e:
                        st.error(f"Error handling request boundary routing inside module: {e}")
                else:
                    st.warning("**MCP Parser Routing Exception:** Prompt input could not be safely mapped to a valid `mcp_server.py` tool. Please use one of the structured commands above.")