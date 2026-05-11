from datetime import datetime
from enum import Enum
import uuid
from pydantic import BaseModel, Field


class MerchantCategory(str, Enum):
    GROCERY = "grocery"
    GAS = "gas"
    RESTAURANT = "restaurant"
    ELECTRONICS = "electronics"
    TRAVEL = "travel"
    ONLINE = "online"
    OTHER = "other"


class Transaction(BaseModel):
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    merchant_id: str
    merchant_category: MerchantCategory
    amount: float
    currency: str = "USD"
    timestamp: datetime = Field(default_factory=lambda: datetime.utcnow())
    country_code: str
    is_card_present: bool
    is_fraud: bool   # ground-truth label — in real systems this is computed offline weeks later