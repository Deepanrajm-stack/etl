import psycopg2
import json

def compare_table_row_counts(config_file='config.json'):
    try:
        # Load table names from JSON config
        with open(config_file, 'r') as file:
            config = json.load(file)
        
        source_table = config["source_table"]
        target_table = config["target_table"]

        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='admin',
            host='localhost',   
            port='5432'
        )
        cursor = connection.cursor()
        
        # Query to get row count for source and target tables
        cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
        source_count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        target_count = cursor.fetchone()[0]

        # Compare the counts
        print(f"Row count for {source_table}: {source_count}")
        print(f"Row count for {target_table}: {target_count}")

        if source_count == target_count:
            print("Both tables have the same number of rows.")
        else:
            print("The tables have different row counts.")

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the database connection
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()

# Call the function
compare_table_row_counts()
