"""
Transaction schema definitions.
Pydantic models for event validation + JSON serialization.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class MerchantCategory(str, Enum):
    GROCERY = "GROCERY"
    ELECTRONICS = "ELECTRONICS"
    TRAVEL = "TRAVEL"
    DINING = "DINING"
    GAS = "GAS"
    ONLINE = "ONLINE"
    ATM = "ATM"
    OTHER = "OTHER"


class TransactionEvent(BaseModel):
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    account_id: str
    amount: float = Field(gt=0, description="Transaction amount in USD")
    merchant_id: str
    merchant_category: MerchantCategory
    merchant_country: str = Field(min_length=2, max_length=2)  # ISO 3166-1 alpha-2
    card_present: bool
    transaction_ts: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    ip_address: Optional[str] = None
    device_fingerprint: Optional[str] = None
    latitude: Optional[float] = Field(default=None, ge=-90, le=90)
    longitude: Optional[float] = Field(default=None, ge=-180, le=180)

    @field_validator("merchant_country")
    @classmethod
    def upper_country(cls, v: str) -> str:
        return v.upper()

    def to_event_hub_bytes(self) -> bytes:
        """Serialize to JSON bytes for Event Hubs ingestion."""
        payload = self.model_dump()
        payload["transaction_ts"] = self.transaction_ts.isoformat()
        payload["merchant_category"] = self.merchant_category.value
        return json.dumps(payload).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "TransactionEvent":
        """Deserialize from Event Hubs message bytes."""
        payload = json.loads(data.decode("utf-8"))
        return cls(**payload)
