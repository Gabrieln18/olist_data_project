from extract import extract_func_csv, extract_func_sqlite
from transform import transform_pipeline_sql
from gold_transform import gold_pipeline_sql
from dotenv import load_dotenv
import logging
import os

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration paths
class Config:
    BRONZE_PATH_OUT = "../tests/bronze"
    SILVER_PATH_OUT = "../tests/silver/"
    GOLD_PATH_OUT = "../tests/gold/"

class OlistPipeline:

    def __init__(self):
        pass

    def extract_bronze(self):
        """Extract all bronze layer data"""
        logger.info("Starting Bronze layer extraction")

        # extração de dados .csv para camada bronze
        extract_func_csv(source_path_csv="../data/olist_customers_dataset.csv", name_table="customers_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_geolocation_dataset.csv", name_table="geolocation_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_order_items_dataset.csv", name_table="order_items_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_order_payments_dataset.csv", name_table="payments_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_order_reviews_dataset.csv", name_table="reviews_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_orders_dataset.csv", name_table="orders_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_products_dataset.csv", name_table="products_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/olist_sellers_dataset.csv", name_table="sellers_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_csv(source_path_csv="../data/product_category_name_translation.csv", name_table="product_category_name_translation_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)

        # extração de dados sqlite, tabelas: leads_closed e leads_qualified
        extract_func_sqlite(db_path="../data/olist.sqlite", sqlite_table="leads_qualified", name_table="leads_qualified_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)
        extract_func_sqlite(db_path="../data/olist.sqlite", sqlite_table="leads_closed", name_table="leads_closed_bronze", bronze_delta_path=Config.BRONZE_PATH_OUT)

        logging.info("Bronze layer extraction completed")

    def transform_silver(self):
        """Transform data from bronze layer to silver layer"""
        logger.info("Starting Silver layer transformation")

        def read_sql(file_path: str):
            with open(file_path, 'r', encoding='utf-8') as file:
                content_sql = file.read()
            return content_sql
        
        silver_sql_files = {
            'customers_sql': read_sql('./models/silver/customers_silver.sql'),
            'geolocation_sql': read_sql('./models/silver/geolocation_silver.sql'),
            'leads_closed_sql': read_sql('./models/silver/leads_closed_silver.sql'),
            'leads_qualified_sql': read_sql('./models/silver/leads_qualified_silver.sql'),
            'order_items_sql': read_sql('./models/silver/order_items_silver.sql'),
            'orders_full_data_sql': read_sql('./models/silver/orders_full_data_silver.sql'),
            'orders_only_delivered_sql': read_sql('./models/silver/orders_only_delivered_silver.sql'),
            'payments_sql': read_sql('./models/silver/payments_silver.sql'),
            'products_sql': read_sql('./models/silver/products_silver.sql'),
            'reviews_sql': read_sql('./models/silver/reviews_silver.sql'),
            'sellers_sql': read_sql('./models/silver/sellers_silver.sql'),
            'agg_customers_sql': read_sql('./models/silver/aggregated_customers_silver.sql'),
            'agg_leads_sql': read_sql('./models/silver/aggregated_leads_silver.sql'),
        }

        # transformação SQL na camada silver do respectivas tabelas
        transform_pipeline_sql(query=silver_sql_files['customers_sql'], table_name="customers_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['geolocation_sql'], table_name="geolocation_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['leads_closed_sql'], table_name="leads_closed_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['leads_qualified_sql'], table_name="leads_qualified_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['order_items_sql'], table_name="order_items_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['orders_full_data_sql'], table_name="orders_full_data_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['orders_only_delivered_sql'], table_name="orders_only_delivered_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['payments_sql'], table_name="payments_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['products_sql'], table_name="products_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['reviews_sql'], table_name="reviews_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['sellers_sql'], table_name="sellers_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['agg_customers_sql'], table_name="aggregated_customers_silver", silver_path_out=Config.SILVER_PATH_OUT)
        transform_pipeline_sql(query=silver_sql_files['agg_leads_sql'], table_name="aggregated_leads_silver", silver_path_out=Config.SILVER_PATH_OUT)


        logger.info("Silver layer transformation completed")

    def transform_gold(self):
        """Transform data from silver layer with business rules to gold layer"""
        logger.info("Starting Gold layer transformation")

        def read_sql(file_path: str):
            with open(file_path, 'r', encoding='utf-8') as file:
                content_sql = file.read()
            return content_sql
        
        gold_sql_files = {
            'agg_sellers_cancellation_rate_sql': read_sql('./models/gold/agg_sellers_cancellation_rate.sql'),
            'agg_sellers_cancellation_rate_v2_sql': read_sql('./models/gold/agg_sellers_cancellation_rate_v2.sql'),
            'freight_analysis_sql': read_sql('./models/gold/freight_analysis.sql'),
            'geo_sales_sql': read_sql('./models/gold/geo_sales.sql'),
            'gold_customers_segmented_sql': read_sql('./models/gold/gold_customers_segmented.sql'),
            'gold_payment_performance_sql': read_sql('./models/gold/gold_payment_performance.sql'),
            'gold_product_inventory_priority_sql': read_sql('./models/gold/gold_product_inventory_priority.sql'),
            'gold_product_profitability_analysis_sql': read_sql('./models/gold/gold_product_profitability_analysis.sql'),
            'gold_qualified_leads_priority_sql': read_sql('./models/gold/gold_qualified_leads_priority.sql'),
            'gold_sellers_order_reviews_score_sql': read_sql('./models/gold/gold_sellers_order_reviews_score.sql'),
            'gold_sellers_shipment_metrics_sql': read_sql('./models/gold/gold_sellers_shipment_metrics.sql'),
            'regional_demand_supply_balance_sql': read_sql('./models/gold/regional_demand_supply_balance.sql'),
            'sales_by_category_sql': read_sql('./models/gold/sales_by_category.sql'),
            'seller_performance_sql': read_sql('./models/gold/seller_performance.sql'),
        }

        # aplicando regra de negócios aos dados: silver -> gold layer
        gold_pipeline_sql(query=gold_sql_files['agg_sellers_cancellation_rate_sql'], gold_table_name="agg_sellers_cancellation_rate", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['agg_sellers_cancellation_rate_v2_sql'], gold_table_name="agg_sellers_cancellation_rate_v2", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['freight_analysis_sql'], gold_table_name="freight_analysis", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['geo_sales_sql'], gold_table_name="geo_sales", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_customers_segmented_sql'], gold_table_name="gold_customers_segmented", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_payment_performance_sql'], gold_table_name="gold_payment_performance", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_product_inventory_priority_sql'], gold_table_name="gold_product_inventory_priority", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_product_profitability_analysis_sql'], gold_table_name="gold_product_profitability_analysis", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_qualified_leads_priority_sql'], gold_table_name="gold_qualified_leads_priority", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_sellers_order_reviews_score_sql'], gold_table_name="gold_sellers_order_reviews_score", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['gold_sellers_shipment_metrics_sql'], gold_table_name="gold_sellers_shipment_metrics", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['regional_demand_supply_balance_sql'], gold_table_name="regional_demand_supply_balance", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['sales_by_category_sql'], gold_table_name="sales_by_category", gold_path_out=Config.GOLD_PATH_OUT)
        gold_pipeline_sql(query=gold_sql_files['seller_performance_sql'], gold_table_name="seller_performance", gold_path_out=Config.GOLD_PATH_OUT)

        logging.info("Gold layer extraction completed")


    def run_pipeline(self, mode='full'):
        """Run the data pipeline
        
        Args:
            mode: 'full', 'bronze', or 'silver'
        """
        if mode in ['full', 'bronze']:
            self.extract_bronze()
            
        if mode in ['full', 'silver']:
            self.transform_silver()

        if mode in ['full', 'gold']:
            self.transform_gold()

def run_bronze():
    """Run only bronze layer extraction"""
    OlistPipeline().run_pipeline('bronze')

def run_silver():
    """Run only silver layer transformation"""
    OlistPipeline().run_pipeline('silver')

def run_gold():
    """Run only silver layer transformation"""
    OlistPipeline().run_pipeline('gold')
    
def run_full():
    """Run complete pipeline"""
    OlistPipeline().run_pipeline('full')

# if __name__ == "__main__":
#     run_full()
    
    