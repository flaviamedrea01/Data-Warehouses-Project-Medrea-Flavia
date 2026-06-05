from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class Region(BaseModel):
    region_id: str = Field(..., alias="Region_ID")
    region_name: str = Field(..., alias="Region_Name")

class Vendor(BaseModel):
    vendor_id: str = Field(..., alias="Vendor_ID")
    vendor_name: str = Field(..., alias="Vendor_Name")
    access_method: str = Field(..., alias="Access_Method")

class FinancialInstrument(BaseModel):
    instrument_id: str = Field(..., alias="Instrument_ID")
    symbol: str = Field(..., alias="Symbol")
    class_type: str = Field(..., alias="Class") 
    region_id: str = Field(..., alias="Region_ID")
    description: str = Field(..., alias="Description")
    
    system_timestamp: datetime = Field(default_factory=datetime.utcnow, alias="System_Timestamp")
    status: str = Field(default="active") 
    
    additional_attributes: Dict[str, Any] = Field(default_factory=dict)

class TimeSeriesPoint(BaseModel):
    entry_id: str = Field(..., alias="Entry_ID")
    instrument_id: str = Field(..., alias="Instrument_ID")
    vendor_id: str = Field(..., alias="Vendor_ID")
    timestamp: datetime = Field(..., alias="Timestamp") 
    
    indicators: Dict[str, Any] = Field(..., description="e.g., {'open': 150, 'close': 152}")
    
    valid_from: datetime = Field(..., alias="Valid_From")
    system_timestamp: datetime = Field(default_factory=datetime.utcnow, alias="System_Timestamp")