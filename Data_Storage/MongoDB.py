from pymongo import MongoClient
from datetime import datetime

# Function to connect to MongoDB
def connect_to_mongo():
    """
    Connect to MongoDB, either locally or on a cloud service like MongoDB Atlas.
    Returns the specified collection for further operations.
    """
    # Replace with MongoDB Atlas connection string if using cloud service
    # client = MongoClient("mongodb_atlas_connection_string")

    # Local MongoDB connection (make sure MongoDB is running locally)
    client = MongoClient("mongodb://localhost:27017/")

    # Select the database and collection
    db = client["diabetes_db"]  # Name of the database
    collection = db["health_data"]  # Name of the collection (table-like structure in MongoDB)
    return collection


# Function to insert a single record into the database
def insert_data(collection, user_id, glucose_level, heart_rate, activity_level=None, food_image_path=None):
    """
    Insert a single record into the MongoDB collection.
    
    Parameters:
        collection (MongoDB collection): The collection where the data will be stored.
        user_id (int): ID of the user.
        glucose_level (float): The user's glucose level.
        heart_rate (float): The user's heart rate.
        activity_level (float, optional): Physical activity level (e.g., steps).
        food_image_path (str, optional): Path to the food image.
    
    Returns:
        None
    """
    # Create a data dictionary to store in MongoDB
    data = {
        "user_id": user_id,
        "timestamp": datetime.utcnow(),  # Current UTC timestamp
        "glucose_level": glucose_level,
        "heart_rate": heart_rate,
        "activity_level": activity_level,
        "food_image_path": food_image_path
    }

    # Insert the data into the collection
    collection.insert_one(data)
    print(f"Data inserted for user_id {user_id}")


# Function to query data from the database
def query_data(collection, user_id=None):
    """
    Query records from the MongoDB collection.
    
    Parameters:
        collection (MongoDB collection): The collection to query from.
        user_id (int, optional): If provided, filters data for the specified user_id.
    
    Returns:
        None (prints the results to the console)
    """
    # Build the query; if user_id is provided, filter by user_id
    query = {}
    if user_id is not None:
        query = {"user_id": user_id}

    # Fetch all matching records
    results = collection.find(query)

    # Print each record
    for record in results:
        print(record)


# Function to update a record in the database
def update_data(collection, user_id, update_fields):
    """
    Update a record in the MongoDB collection for a specific user.
    
    Parameters:
        collection (MongoDB collection): The collection to update.
        user_id (int): ID of the user whose data needs to be updated.
        update_fields (dict): Dictionary of fields to update with their new values.
    
    Returns:
        None
    """
    # Use MongoDB's $set operator to update specific fields
    collection.update_one({"user_id": user_id}, {"$set": update_fields})
    print(f"Data for user_id {user_id} updated with fields: {update_fields}")


# Function to delete a record from the database
def delete_data(collection, user_id):
    """
    Delete a record from the MongoDB collection for a specific user.
    
    Parameters:
        collection (MongoDB collection): The collection to delete from.
        user_id (int): ID of the user whose data needs to be deleted.
    
    Returns:
        None
    """
    # Find the record with the specified user_id and delete it
    collection.delete_one({"user_id": user_id})
    print(f"Data for user_id {user_id} deleted.")


# Function to bulk insert multiple records into the database
def bulk_insert_data(collection, data_list):
    """
    Insert multiple records into the MongoDB collection in a single operation.
    
    Parameters:
        collection (MongoDB collection): The collection where the data will be stored.
        data_list (list of dict): List of dictionaries, each representing a record.
    
    Returns:
        None
    """
    # Insert multiple records at once
    collection.insert_many(data_list)
    print(f"{len(data_list)} records inserted.")


# Main function to demonstrate the above functionalities
if __name__ == "__main__":
    # Connect to MongoDB
    collection = connect_to_mongo()

    # Insert a single record
    insert_data(
        collection,
        user_id=1,
        glucose_level=110.5,
        heart_rate=72,
        activity_level=5000,
        food_image_path="/path/to/image1.jpg"
    )

    # Insert multiple records
    bulk_data = [
        {"user_id": 2, "glucose_level": 120.3, "heart_rate": 75, "activity_level": 3000, "food_image_path": "/path/to/image2.jpg"},
        {"user_id": 3, "glucose_level": 100.8, "heart_rate": 80, "activity_level": 7000, "food_image_path": "/path/to/image3.jpg"}
    ]
    bulk_insert_data(collection, bulk_data)

    # Query all data
    print("All data in the collection:")
    query_data(collection)

    # Query data for a specific user
    print("Data for user_id 1:")
    query_data(collection, user_id=1)

    # Update a specific user's data
    update_data(collection, user_id=1, update_fields={"glucose_level": 115.0})

    # Query updated data for user_id 1
    print("Updated data for user_id 1:")
    query_data(collection, user_id=1)

    # Delete a user's data
    delete_data(collection, user_id=3)

    # Query all data after deletion
    print("All data after deletion:")
    query_data(collection)
