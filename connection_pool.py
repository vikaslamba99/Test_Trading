#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 19:20:22 2020

@author: vikaslamba
"""
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
import mysql.connector.pooling
import sqlalchemy

database_username = 'root'
database_password = 'V!shnuPurana36'
database_ip       = 'localhost'
database_name     = 'Time_Series'
    
def get_connection():
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=3, pool_reset_session=True, host = database_ip, database = database_name, user = database_username, password = database_password)
    # Get connection object from a pool
    connection_object = connection_pool.get_connection()
    return connection_object


def get_con_alchemy():
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name))
    database_connection = database_connection.execution_options(isolation_level="AUTOCOMMIT")
    return database_connection
    
    
#        if connection_object.is_connected():
#           db_Info = connection_object.get_server_info()
#           print("Connected to MySQL database using connection pool ... MySQL Server version on ",db_Info)
#    
#           cursor = connection_object.cursor()
#           cursor.execute("select database();")
#           record = cursor.fetchone()
#           print ("Your connected to - ", record)
#    
#    except Error as e :
#        print ("Error while connecting to MySQL using Connection pool ", e)
#    finally:
#        #closing database connection.
#        if(connection_object.is_connected()):
#            cursor.close()
#            connection_object.close()
#            print("MySQL connection is closed")