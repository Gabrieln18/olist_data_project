from ingestion_pipeline import IngestionPipeline

#  * Olist Data Project
#  *
#  * This source code is licensed under the MIT license found in the
#  * LICENSE file in the root directory of this source tree.
#  * This code was developed by Gabriel Nunes.

if __name__ == "__main__":

    # INGESTION PIPELINES
    customers_ingestion = IngestionPipeline(
        name_pipeline="customers_ingestion",
        name_table="raw_customers_table",
        path_csv="../data/olist_customers_dataset.csv",
        path_delta="./RAW/customers"
    )
    geoloc_ingestion = IngestionPipeline(
        name_pipeline="geolocation_ingestion",
        name_table="raw_geolocation_table",
        path_csv="../data/olist_geolocation_dataset.csv",
        path_delta="./RAW/geolocation"
    )
    order_items_ingestion = IngestionPipeline(
        name_pipeline="order_items_ingestion",
        name_table="raw_order_items_table",
        path_csv="../data/olist_order_items_dataset.csv",
        path_delta="./RAW/order_items"
    )
    payments_ingestion = IngestionPipeline(
        name_pipeline="order_items_ingestion",
        name_table="raw_order_items_table",
        path_csv="../data/olist_order_items_dataset.csv",
        path_delta="./RAW/order_items"
    )
    reviews_ingestion = IngestionPipeline(
        name_pipeline="reviews_ingestion",
        name_table="raw_reviews_table",
        path_csv="../data/olist_order_reviews_dataset.csv",
        path_delta="./RAW/order_reviews"
    )
    orders_ingestion = IngestionPipeline(
        name_pipeline="orders_ingestion",
        name_table="raw_orders_table",
        path_csv="../data/olist_orders_dataset.csv",
        path_delta="./RAW/orders"
    )
    products_ingestion = IngestionPipeline(
        name_pipeline="products_ingestion",
        name_table="raw_products_table",
        path_csv="../data/olist_products_dataset.csv",
        path_delta="./RAW/products"
    )
    sellers_ingestion = IngestionPipeline(
        name_pipeline="sellers_ingestion",
        name_table="raw_sellers_table",
        path_csv="../data/olist_sellers_dataset.csv",
        path_delta="./RAW/sellers"
    )
    product_category_ingestion = IngestionPipeline(
        name_pipeline="product_category_ingestion",
        name_table="raw_product_category_table",
        path_csv="../data/product_category_name_translation.csv",
        path_delta="./RAW/product_category_name"
    )
    
    # init pipelines
    pipeline_ingestion_list = [customers_ingestion, geoloc_ingestion,
                               order_items_ingestion, payments_ingestion,
                               reviews_ingestion, orders_ingestion, products_ingestion,
                               sellers_ingestion, product_category_ingestion]
    
    for pipeline_ingestion in pipeline_ingestion_list:
        pipeline_ingestion.ingestion_to_bronze_delta()
    