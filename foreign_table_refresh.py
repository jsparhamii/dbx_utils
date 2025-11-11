# Databricks notebook source
dbutils.widgets.text("catalog", "your_catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import concurrent.futures

dbutils.widgets.text("catalog", "your_catalog")
catalog = dbutils.widgets.get("catalog")

def get_foreign_tables(catalog):
    """
    Return a list of tuples (schema_name, table_name) for all foreign tables in a catalog.
    Queries system.information_schema.tables to filter by table type = 'FOREIGN'.
    """
    try:
        query = f"""
            SELECT table_schema, table_name
            FROM system.information_schema.tables
            WHERE table_type = 'FOREIGN' and table_catalog = '{catalog}' and table_schema not in ('sys', 'information_schema')
        """
        df = spark.sql(query)
        return [(row['table_schema'], row['table_name']) for row in df.collect()]
    except Exception as e:
        print(f"Failed to query information_schema for catalog {catalog}: {e}")
        return []

def refresh_table(catalog, schema_table):
    """Refresh a single foreign table with error handling."""
    schema, table = schema_table
    full_table_name = f"{catalog}.{schema}.{table}"
    try:
        spark.sql(f"REFRESH FOREIGN TABLE {full_table_name}")
        print(f"[SUCCESS] Refreshed {full_table_name}")
    except Exception as e:
        print(f"[FAILED] Could not refresh {full_table_name}: {e}")

def main(catalog, max_workers=8):
    tables = get_foreign_tables(catalog)
    
    if not tables:
        print(f"No foreign tables found in catalog {catalog}")
        return
    
    print(f"Refreshing {len(tables)} foreign tables in catalog {catalog}...")
    
    # Run refresh in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(refresh_table, catalog, t) for t in tables]
        concurrent.futures.wait(futures)
    
    print("Refresh process complete.")

if __name__ == "__main__":
    catalog_name = catalog  # Replace with your catalog
    main(catalog_name)
