from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from decimal import Decimal

@dataclass
class RawSale:
    sale_id: str
    sale_date: str
    customer_id: str
    product: str
    quantity: int
    unit_price: float
    category: str
    region: str

@dataclass
class Customer:
    customer_id: str
    name: str
    email: str
    city: str
    state: str
    segment: str

@dataclass
class ProcessedSale:
    sale_id: str
    sale_date: datetime
    customer_id: str
    customer_name: str
    product: str
    quantity: int
    unit_price: Decimal
    total_value: Decimal
    category: str
    region: str
    customer_state: str
    customer_segment: str
    day_of_week: str
    month: str
    year: int