import logging
import csv
import json
from pathlib import Path

from extract.extractors import SalesExtractor, CustomersExtractor
from transform.transformers import DataTransformer
from load.loaders import SQLiteLoader, CSVLoader, JSONLoader
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asma)s - %(levelname)s - %(message)s'
)


def create_sample_data():
    """Creates sample data for testing"""
    input_dir = settings.INPUT_DIR
    input_dir.mkdir(parents=True, exist_ok=True)

    # Create sales CSV
    sales_csv = input_dir / settings.SALES_CSV
    sales_data = [
        ['sale_id', 'sale_date', 'customer_id', 'product', 'quantity', 'unit_price', 'category', 'region'],
        ['S001', '2024-01-15', 'C001', 'Gaming Laptop', 1, 2500.00, 'Electronics', 'Southeast'],
        ['S002', '2024-01-16', 'C002', 'Wireless Mouse', 2, 89.90, 'Accessories', 'South'],
        ['S003', '2024-01-17', 'C003', 'Mechanical Keyboard', 1, 299.00, 'Accessories', 'Northeast'],
        ['S004', '2024-01-18', 'C001', '24" Monitor', 1, 899.00, 'Electronics', 'Southeast'],
        ['S005', '2024-01-19', 'C004', 'Bluetooth Headphones', 3, 199.00, 'Audio', 'Midwest']
    ]

    with open(sales_csv, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(sales_data)

    # Create customers JSON
    customers_json = input_dir / settings.CUSTOMERS_JSON
    customers_data = [
        {
            "customer_id": "C001",
            "name": "John Smith",
            "email": "john.smith@email.com",
            "city": "São Paulo",
            "state": "SP",
            "segment": "Corporate"
        },
        {
            "customer_id": "C002",
            "name": "Maria Santos",
            "email": "maria.santos@email.com",
            "city": "Porto Alegre",
            "state": "RS",
            "segment": "Home Office"
        },
        {
            "customer_id": "C003",
            "name": "Peter Oliveira",
            "email": "peter.oliveira@email.com",
            "city": "Salvador",
            "state": "BA",
            "segment": "Gamer"
        },
        {
            "customer_id": "C004",
            "name": "Anna Costa",
            "email": "anna.costa@email.com",
            "city": "Brasília",
            "state": "DF",
            "segment": "Home Office"
        }
    ]

    with open(customers_json, 'w', encoding='utf-8') as file:
        json.dump(customers_data, file, indent=2, ensure_ascii=False)

    print("📝 Sample data created!")


class ETLSystem:
    """Main ETL system"""

    def __init__(self):
        self.sales_extractor = SalesExtractor()
        self.customers_extractor = CustomersExtractor()
        self.data_transformer = DataTransformer()
        self.sqlite_loader = SQLiteLoader()
        self.csv_loader = CSVLoader()
        self.json_loader = JSONLoader()

    def execute(self):
        """Executes the complete ETL pipeline"""
        print("🚀 Starting ETL system...")

        # Check files
        if not self.sales_extractor.check_files():
            print("❌ Input files not found! Creating sample data...")
            create_sample_data()

        # Extraction
        print("📥 Extracting data...")
        raw_sales = self.sales_extractor.extract_sales()
        customers = self.customers_extractor.extract_customers()

        if not raw_sales or not customers:
            print("❌ Data extraction failed!")
            return

        # Transformation
        print("🔄 Transforming data...")
        processed_sales = self.data_transformer.transform_data(raw_sales, customers)

        if not processed_sales:
            print("❌ No data processed!")
            return

        # Loading
        print("📤 Loading data...")
        self.sqlite_loader.load_to_sqlite(processed_sales)
        self.csv_loader.load_to_csv(processed_sales)
        self.json_loader.load_to_json(processed_sales)

        print("✅ ETL process completed successfully!")


if __name__ == "__main__":
    system = ETLSystem()
    system.execute()