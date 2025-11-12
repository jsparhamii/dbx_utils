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
    
    schemas = [schema_name.databaseName for schema_name in spark.sql(f"SHOW SCHEMAS IN {catalog}").collect() if schema_name.databaseName not in ('sys', 'information_schema')]
    
    tables = []
    for schema in schemas:
        tables.extend([f"{catalog}.{schema}.{table['tableName']}" for table in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()])
    
    return tables
    

def refresh_table(fqn):
    """Refresh a single foreign table with error handling."""
    
    full_table_name = fqn
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
        futures = [executor.submit(refresh_table, t) for t in tables]
        concurrent.futures.wait(futures)
    
    print("Refresh process complete.")

if __name__ == "__main__":
    catalog_name = catalog  # Replace with your catalog
    main(catalog_name)
