import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Import transformations from pipeline modules
from pipelines.bronze import extractors
from pipelines.silver import (
    customers_transform, geolocation_transform, order_items_transform,
    orders_transform, payments_transform, products_transform,
    reviews_transform, sellers_transform, leads_transform
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration paths
class Config:
    BRONZE_PATH = os.getenv('DELTA_LAKE_BRONZE_PATH_TEST')
    SILVER_PATH = os.getenv('DELTA_LAKE_SILVER_PATH_TEST')
    DATA_PATH = os.getenv('DATA_PATH_REPOSITORY')
    SQLITE_PATH = os.getenv('SQLITE_DATA_PATH')

class OlistPipeline:
    def __init__(self, bronze_path=Config.BRONZE_PATH, silver_path=Config.SILVER_PATH,
                 data_path=Config.DATA_PATH, sqlite_path=Config.SQLITE_PATH):
        self.paths = {
            'bronze': bronze_path,
            'silver': silver_path,
            'data': data_path,
            'sqlite': sqlite_path
        }
        
        # Create directories
        Path(bronze_path).mkdir(parents=True, exist_ok=True)
        Path(silver_path).mkdir(parents=True, exist_ok=True)
        
    def extract_bronze(self):
        """Extract all bronze layer data"""
        logger.info("Starting Bronze layer extraction")
        bronze_path = self.paths['bronze']
        data_path = self.paths['data']
        sqlite_path = self.paths['sqlite']
        
        # Extract CSV files
        extractors.extract_csv(f"{data_path}/olist_customers_dataset.csv", bronze_path, "customers_bronze")
        extractors.extract_csv(f"{data_path}/olist_geolocation_dataset.csv", bronze_path, "geolocation_bronze")
        extractors.extract_csv(f"{data_path}/olist_order_items_dataset.csv", bronze_path, "order_items_bronze")
        extractors.extract_csv(f"{data_path}/olist_order_payments_dataset.csv", bronze_path, "payments_bronze")
        extractors.extract_csv(f"{data_path}/olist_order_reviews_dataset.csv", bronze_path, "reviews_bronze")
        extractors.extract_csv(f"{data_path}/olist_orders_dataset.csv", bronze_path, "orders_bronze")
        extractors.extract_csv(f"{data_path}/olist_products_dataset.csv", bronze_path, "products_bronze")
        extractors.extract_csv(f"{data_path}/olist_sellers_dataset.csv", bronze_path, "sellers_bronze")
        extractors.extract_csv(f"{data_path}/product_category_name_translation.csv", bronze_path, 
                               "product_category_name_translation_bronze")
        
        # Extract SQLite data
        extractors.extract_sqlite(sqlite_path, "leads_qualified", bronze_path, "leads_qualified_bronze")
        extractors.extract_sqlite(sqlite_path, "leads_closed", bronze_path, "leads_closed_bronze")
        
        logger.info("Bronze layer extraction completed")
    
    def transform_silver(self):
        """Transform data to silver layer"""
        logger.info("Starting Silver layer transformation")
        silver_path = self.paths['silver']
        
        # Call each transformation directly
        customers_transform.create_customers_silver(delta_path=os.getenv("DELTA_LAKE_BRONZE_CUSTOMERS"), silver_path_out=Config.SILVER_PATH)
        geolocation_transform.create_geolocation_silver(delta_path=os.getenv("DELTA_LAKE_BRONZE_GEOLOC"), silver_path_out=Config.SILVER_PATH)
        order_items_transform.create_order_items_silver(delta_path=os.getenv("DELTA_LAKE_BRONZE_ORDER_ITEMS"), silver_path_out=Config.SILVER_PATH)
        orders_transform.create_orders_delivered_silver(delta_path=os.getenv("DELTA_LAKE_BRONZE_GEOLOC"), silver_path_out=Config.SILVER_PATH)
        orders_transform.create_orders_full_silver(delta_path=os.getenv("DELTA_LAKE_BRONZE_GEOLOC"), silver_path_out=Config.SILVER_PATH)
        payments_transform.create_payments_silver(silver_path_out=Config.SILVER_PATH)
        products_transform.create_products_silver(silver_path_out=Config.SILVER_PATH)
        reviews_transform.create_order_reviews(silver_path_out=Config.SILVER_PATH)
        sellers_transform.create_sellers_silver(silver_path_out=Config.SILVER_PATH)
        leads_transform.create_leads_qualified_silver(silver_path_out=Config.SILVER_PATH)
        leads_transform.create_leads_closed_silver(silver_path_out=Config.SILVER_PATH)
        
        logger.info("Silver layer transformation completed")
    
    def run_pipeline(self, mode='full'):
        """Run the data pipeline
        
        Args:
            mode: 'full', 'bronze', or 'silver'
        """
        if mode in ['full', 'bronze']:
            self.extract_bronze()
            
        if mode in ['full', 'silver']:
            self.transform_silver()

# Helper functions
def run_bronze():
    """Run only bronze layer extraction"""
    OlistPipeline().run_pipeline('bronze')

def run_silver():
    """Run only silver layer transformation"""
    OlistPipeline().run_pipeline('silver')

def run_full():
    """Run complete pipeline"""
    OlistPipeline().run_pipeline('full')

if __name__ == "__main__":
    # Uncomment one of these to run different pipeline modes
    # run_full()
    run_silver()
    # run_bronze()