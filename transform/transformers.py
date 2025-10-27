from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict
import logging

from models.data_models import RawSale, Customer, ProcessedSale


class DataTransformer:
    """Transforms raw data into processed data"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def transform_data(
            self,
            sales: List[RawSale],
            customers: Dict[str, Customer]
    ) -> List[ProcessedSale]:
        """Transforms raw data into processed data"""
        processed_sales = []

        for sale in sales:
            try:
                processed_sale = self._process_sale(sale, customers)
                if processed_sale:
                    processed_sales.append(processed_sale)
            except Exception as e:
                self.logger.warning(f"Error processing sale {sale.sale_id}: {e}")
                continue

        print(f"✅ Processed {len(processed_sales)} sales")
        return processed_sales

    def _process_sale(
            self,
            sale: RawSale,
            customers: Dict[str, Customer]
    ) -> ProcessedSale | None:
        """Processes an individual sale"""

        # Validate basic data
        if not self._validate_sale(sale):
            return None

        # Get customer information
        customer = customers.get(sale.customer_id)
        if not customer:
            self.logger.warning(f"Customer not found: {sale.customer_id}")
            return None

        # Convert date
        try:
            sale_date = datetime.strptime(sale.sale_date, "%Y-%m-%d")
        except ValueError:
            self.logger.warning(f"Invalid date: {sale.sale_date}")
            return None

        # Calculate values
        unit_price = Decimal(str(sale.unit_price)).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        total_value = (unit_price * sale.quantity).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )

        # Extract temporal information
        day_of_week = sale_date.strftime("%A")
        month = sale_date.strftime("%B")
        year = sale_date.year

        return ProcessedSale(
            sale_id=sale.sale_id,
            sale_date=sale_date,
            customer_id=sale.customer_id,
            customer_name=customer.name,
            product=sale.product,
            quantity=sale.quantity,
            unit_price=unit_price,
            total_value=total_value,
            category=sale.category,
            region=sale.region,
            customer_state=customer.state,
            customer_segment=customer.segment,
            day_of_week=day_of_week,
            month=month,
            year=year
        )

    def _validate_sale(self, sale: RawSale) -> bool:
        """Validates sale data"""
        if sale.quantity <= 0:
            return False
        if sale.unit_price <= 0:
            return False
        if not sale.sale_id or not sale.customer_id:
            return False
        return True