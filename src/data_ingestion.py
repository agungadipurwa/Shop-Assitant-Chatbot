import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
# Load environment variables from a .env file


# Read the CSV file
csv_file_path = 'C:/D/Project/Shop-Assitant-Chatbot/data/cleaned_products_catalog.csv'
data = pd.read_csv(csv_file_path)

# Connect to PostgreSQL
db_connection = psycopg2.connect(
            database="product_catalog_db",
            user="postgres",
            password=os.getenv('DB_PASSWORD'),
            host="127.0.0.1",
            port=5432,
        )

cursor = db_connection.cursor()


# Insert data into PostgreSQL
for index, row in data.iterrows():
    sql = """
    INSERT INTO products (ProductID, ProductName, ProductBrand, Gender, Price, Description, PrimaryColor)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, tuple(row))



db_connection.commit()

cursor.close()
db_connection.close()