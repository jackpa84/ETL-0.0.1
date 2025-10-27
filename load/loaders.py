import sqlite3
import csv
import json
from pathlib import Path
from typing import List

from config.settings import settings
from models.data_models import ProcessedSale


class DataLoader:
    """Base class for data loading"""

    def __init__(self):
        self.output_dir = settings.OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)


class SQLiteLoader(DataLoader):
    """Loads data to SQLite"""

    def create_table(self, connection: sqlite3.Connection):
        """Creates table in database"""
        cursor = connection.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS processed_sales (
            sale_id TEXT PRIMARY KEY,
            sale_date DATE,
            customer_id TEXT,
            customer_name TEXT,
            product TEXT,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_value DECIMAL(10,2),
            category TEXT,
            region TEXT,
            customer_state TEXT,
            customer_segment TEXT,
            day_of_week TEXT,
            month TEXT,
            year INTEGER
        )
        """
        cursor.execute(create_table_query)
        connection.commit()

    def load_to_sqlite(self, sales: List[ProcessedSale]):
        """Loads data to SQLite"""
        db_path = self.output_dir / settings.OUTPUT_DB

        try:
            with sqlite3.connect(db_path) as connection:
                self.create_table(connection)

                cursor = connection.cursor()
                insert_query = """
                INSERT OR REPLACE INTO processed_sales 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                data = [
                    (
                        sale.sale_id,
                        sale.sale_date.strftime("%Y-%m-%d"),
                        sale.customer_id,
                        sale.customer_name,
                        sale.product,
                        sale.quantity,
                        float(sale.unit_price),
                        float(sale.total_value),
                        sale.category,
                        sale.region,
                        sale.customer_state,
                        sale.customer_segment,
                        sale.day_of_week,
                        sale.month,
                        sale.year
                    )
                    for sale in sales
                ]

                cursor.executemany(insert_query, data)
                connection.commit()

            print(f"✅ Data loaded to SQLite: {db_path}")
        except Exception as e:
            print(f"❌ Error loading to SQLite: {e}")


class CSVLoader(DataLoader):
    """Loads data to CSV"""

    def load_to_csv(self, sales: List[ProcessedSale]):
        """Loads data to CSV"""
        csv_path = self.output_dir / "processed_sales.csv"

        try:
            data = [
                {
                    'sale_id': sale.sale_id,
                    'sale_date': sale.sale_date.strftime("%Y-%m-%d"),
                    'customer_id': sale.customer_id,
                    'customer_name': sale.customer_name,
                    'product': sale.product,
                    'quantity': sale.quantity,
                    'unit_price': float(sale.unit_price),
                    'total_value': float(sale.total_value),
                    'category': sale.category,
                    'region': sale.region,
                    'customer_state': sale.customer_state,
                    'customer_segment': sale.customer_segment,
                    'day_of_week': sale.day_of_week,
                    'month': sale.month,
                    'year': sale.year
                }
                for sale in sales
            ]

            with open(csv_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)

            print(f"✅ Data loaded to CSV: {csv_path}")
        except Exception as e:
            print(f"❌ Error loading to CSV: {e}")


class JSONLoader(DataLoader):
    """Loads data to JSON"""

    def load_to_json(self, sales: List[ProcessedSale]):
        """Loads data to JSON"""
        json_path = self.output_dir / "processed_sales.json"

        try:
            data = [
                {
                    'sale_id': sale.sale_id,
                    'sale_date': sale.sale_date.strftime("%Y-%m-%d"),
                    'customer_id': sale.customer_id,
                    'customer_name': sale.customer_name,
                    'product': sale.product,
                    'quantity': sale.quantity,
                    'unit_price': float(sale.unit_price),
                    'total_value': float(sale.total_value),
                    'category': sale.category,
                    'region': sale.region,
                    'customer_state': sale.customer_state,
                    'customer_segment': sale.customer_segment,
                    'day_of_week': sale.day_of_week,
                    'month': sale.month,
                    'year': sale.year
                }
                for sale in sales
            ]

            with open(json_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)

            print(f"✅ Data loaded to JSON: {json_path}")
        except Exception as e:
            print(f"❌ Error loading to JSON: {e}")