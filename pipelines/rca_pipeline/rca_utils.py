
from datetime import datetime, timedelta
import re

def generate_journal_name(date: datetime) -> str:
    """Generate journal name string based on input date quarter."""
    year_short = date.strftime("%y")
    month = date.month
    if 1 <= month <= 3:
        start_month, end_month = 1, 3
    elif 4 <= month <= 6:
        start_month, end_month = 4, 6
    elif 7 <= month <= 9:
        start_month, end_month = 7, 9
    else:
        start_month, end_month = 10, 12
    return f"journals_{year_short}_{start_month:02d}_{end_month:02d}"

def to_snake_case(name: str) -> str:
    # Replace spaces and camelCase with snake_case
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)  # handle camelCase
    name = re.sub(r'[\s\-]+', '_', name)  # handle spaces or dashes
    return name.lower()


def load_to_postgres_database(df, table_name,postgres_host, postgres_port, postgres_database, postgres_user, postgres_password):
    """
    Loads a processed RCA Spark DataFrame into a PostgreSQL table."""
    
    
    print("Loading RCA data into Data Warehouse (Postgres)...")
    
    # 1. Get the DataFrame schema
    '''schema = df.schema
    
    # 2. Build the column type string for the JDBC writer
    # This is crucial for Spark to correctly create the table if it doesn't exist.
    column_types = []
    for field in schema.fields:
        # Map Spark data types to PostgreSQL data types.
        # This is a simplified example; you might need a more comprehensive mapping.
        if isinstance(field.dataType, StringType):
            db_type = "Text" # Changed from VARCHAR(255) to TEXT for unlimited length
        else:
            db_type = "Text" # Defaulting to a generic type for other types
            
        column_types.append(f'{field.name} {db_type}')
    
    create_table_column_types_string = ", ".join(column_types)'''

    writer = df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
        .option("dbtable", f"ptsp_schema.{table_name}") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") # Use append mode, which creates the table if it doesn't exist

    try:
        writer.save()
        print(f"RCA Data successfully loaded into ptsp_schema.{table_name}.")
        return True
    except Exception as e:
        print(f"Error loading RCA data: {e}")
        # Add more specific error handling here if needed.