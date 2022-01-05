#!/usr/bin/env python3

import mysql.connector as mysql

def connectdb():
    try:
        conn = mysql.connect(user='sd',password='Deepu@123',database='samiti',host='127.0.0.1')
        #check the connection with database
        if conn.is_connected():
            print("Database Connection is established")
        return conn
    except:
        raise