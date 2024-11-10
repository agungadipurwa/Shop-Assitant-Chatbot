import os
import psycopg2
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from tqdm.auto import tqdm
import time
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()


# Pinecone configuration
api_key = os.getenv("PINECONE_API_KEY")
pc = Pinecone(api_key=api_key)

spec = ServerlessSpec(
    cloud="aws", region="us-east-1"
)

index_name = 'product-catalog-index'
existing_indexes = [index_info["name"] for index_info in pc.list_indexes()]

# Check if index already exists (it shouldn't if this is the first time)
if index_name not in existing_indexes:
    # If it does not exist, create the index
    pc.create_index(
        name=index_name,
        dimension=768,  # dimensionality of the embedding model
        metric='dotproduct',
        spec=spec
    )
    # Wait for the index to be initialized
    while not pc.describe_index(index_name).status['ready']:
        time.sleep(1)

# Connect to the index
index = pc.Index(index_name)
time.sleep(1)

# MySQL configuration
db_connection = psycopg2.connect(
            database="product_catalog_db",
            user="postgres",
            password=os.getenv('DB_PASSWORD'),
            host="127.0.0.1",
            port=5432,
        )
cursor = db_connection.cursor()

# Google Generative AI API configuration
os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")
embed_model = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

def fetch_data():
    query = "SELECT * FROM products"
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = pd.DataFrame(cursor.fetchall(), columns=columns)

    return data

def sync_with_pinecone(data):
    batch_size = 100
    total_batches = (len(data) + batch_size - 1) // batch_size

    for i in tqdm(range(0, len(data), batch_size), desc='Processing Batches', unit='batch', total=total_batches):
        i_end = min(len(data), i + batch_size)
        batch = data.iloc[i:i_end]

        # Generate unique IDs
        ids = [str(row['productid']) for _, row in batch.iterrows()]

        # Combine text fields for embedding
        texts = [
            f"{row['description']} {row['productname']} {row['productbrand']} {row['gender']} {row['price']} {row['primarycolor']}"
            for _, row in batch.iterrows()
        ]

        # Embed text
        embeds = embed_model.embed_documents(texts)

        # Get metadata to store in Pinecone
        metadata = [
            {
                'ProductName': row['productname'],
                'ProductBrand': row['productbrand'],
                'Gender': row['gender'],
                'Price': row['price'],
                'Description': row['description'],
                'PrimaryColor': row['primarycolor']
            }
            for _, row in batch.iterrows()
        ]

        # Upserting Vectors
        with tqdm(total=len(ids), desc='Upserting Vectors', unit='vector') as upsert_pbar:
            index.upsert(vectors=zip(ids, embeds, metadata))
            upsert_pbar.update(len(ids))  # Update the upsert progress bar

def main():
    data = fetch_data()
    sync_with_pinecone(data)

if __name__ == "__main__":
    main()

# Close the cursor and connection
cursor.close()
db_connection.close()