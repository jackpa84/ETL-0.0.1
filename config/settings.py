from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    BASE_DIR: Path = Path(__file__).parent.parent
    INPUT_DIR: Path = BASE_DIR / "data" / "input"
    OUTPUT_DIR: Path = BASE_DIR / "data" / "output"

    SALES_CSV: str = "sales.csv"
    CUSTOMERS_JSON: str = "customers.json"
    OUTPUT_DB: str = "processed_sales.db"

    CHUNK_SIZE: int = 1000
    DATE_FORMAT: str = "%Y-%m-%d"


settings = Settings()