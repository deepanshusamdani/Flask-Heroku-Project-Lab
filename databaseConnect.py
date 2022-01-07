#!/usr/bin/env python3
import psycopg2

def connectdb():
    try: 
        conn = psycopg2.connect(database='d3sp7emltr5ruq', 
                                user='bhbfatufvucgzo', 
                                password='b2f9d1762fa505f7d975ade20047694d7752f613b9948201153c5896fb88aae9', 
                                host='ec2-34-239-196-254.compute-1.amazonaws.com', 
                                port= '5432')
        cursor = conn.cursor()
        if conn is not None:
            print("Database Connection is established")
        return conn
    except:
        raise
