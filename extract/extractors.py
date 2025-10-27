import csv
import json
from pathlib import Path
from typing import List, Dict
import sqlite3

from config.settings import settings
from models.data_models import RawSale, Customer


class DataExtractor:
    """Base class for data extraction"""

    def __init__(self):
        self.input_dir = settings.INPUT_DIR

    def check_files(self) -> bool:
        """Checks if input files exist"""
        return all([
            (self.input_dir / settings.SALES_CSV).exists(),
            (self.input_dir / settings.CUSTOMERS_JSON).exists()
        ])


class SalesExtractor(DataExtractor):
    """Extracts sales data from CSV"""

    def extract_sales(self) -> List[RawSale]:
        """Extracts sales from CSV file"""
        sales = []
        sales_file = self.input_dir / settings.SALES_CSV

        try:
            with open(sales_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    sale = RawSale(
                        sale_id=row['sale_id'],
                        sale_date=row['sale_date'],
                        customer_id=row['customer_id'],
                        product=row['product'],
                        quantity=int(row['quantity']),
                        unit_price=float(row['unit_price']),
                        category=row['category'],
                        region=row['region']
                    )
                    sales.append(sale)
            print(f"✅ Extracted {len(sales)} sales")
            return sales
        except Exception as e:
            print(f"❌ Error extracting sales: {e}")
            return []


class CustomersExtractor(DataExtractor):
    """Extracts customer data from JSON"""

    def extract_customers(self) -> Dict[str, Customer]:
        """Extracts customers from JSON file"""
        customers = {}
        customers_file = self.input_dir / settings.CUSTOMERS_JSON

        try:
            with open(customers_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
                for customer_data in data:
                    customer = Customer(
                        customer_id=customer_data['customer_id'],
                        name=customer_data['name'],
                        email=customer_data['email'],
                        city=customer_data['city'],
                        state=customer_data['state'],
                        segment=customer_data['segment']
                    )
                    customers[customer.customer_id] = customer
            print(f"✅ Extracted {len(customers)} customers")
            return customers
        except Exception as e:
            print(f"❌ Error extracting customers: {e}")
            return {}