# Author: Vikas Lamba
# Date: 2025-08-14
# Description: This module handles the connection to the MySQL database.

import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

# --- Database Credentials ---
DB_CONFIG = {
    'host': '145.223.74.215',
    'user': 'algoadmin',
    'password': 'c1yhuFt9FdJ7SbZGQm1t',
    'database': 'algotrader'
}

def get_db_connection():
    """
    Establishes a connection to the MySQL database.

    Returns:
        mysql.connector.connection.MySQLConnection: A connection object or None if connection fails.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def get_db_engine():
    """
    Creates a SQLAlchemy engine for the MySQL database.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy engine object or None if creation fails.
    """
    try:
        db_uri = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        print(f"Error creating SQLAlchemy engine: {e}")
        return None

if __name__ == '__main__':
    # This block is for testing the connection.
    # It will only run when the script is executed directly.
    print("Testing database connection...")
    connection = get_db_connection()
    if connection:
        print("MySQL Database connection successful!")
        connection.close()
        print("Connection closed.")
    else:
        print("MySQL Database connection failed.")

    print("\nTesting SQLAlchemy engine...")
    engine = get_db_engine()
    if engine:
        try:
            with engine.connect() as connection:
                print("SQLAlchemy engine connection successful!")
            print("Connection closed.")
        except Exception as e:
            print(f"SQLAlchemy engine connection failed: {e}")
    else:
        print("SQLAlchemy engine creation failed.")
