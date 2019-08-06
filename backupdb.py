# ─── AUTO BACKUP HADOOP TO LOCAL DATABASE MONTHLY RAW DATA ──────────────────────
#
# Hendriktio Freizello
# ────────────────────────────────────────────────────────────────────────────────

import datetime as dtt
import pyodbc
import csv
import regex as re
import os

def hdp_connect():
    pyodbc.autocommit = True
    pyodbc.pooling = False

    print ('Connecting Hadoop...')
    conn = pyodbc.connect("DSN=hive64", autocommit=True)
    # cursor = conn.cursor()
    print ('Hadoop Connected')
    return conn

def local_connect():
    pyodbc.autocommit = True
    pyodbc.pooling = False

    print ('Connecting Local MySQL...')
    conn = pyodbc.connect("DSN=ubuntu-mariadb-dev", autocommit=True)
    # cursor = conn.cursor()
    print ('Local MySQL Connected')
    return conn

#1 Generate header sql create table from hadoop
def create_table_script(conn, schema, tblname): 
    cursor = conn.cursor()

    # start
    table_columns = cursor.columns(schema=schema, table=tblname)

    print ('Create Header SQL Create Table Script...')
    sql = "CREATE TABLE "+tblname+"_TEST "
    columns = "("

    for row in table_columns:
        dtype = row.type_name
        dsize = str(row.buffer_length)
        ddecimal = str(row.decimal_digits)

        if ddecimal == None or ddecimal == "None" :
            ddecimal = "0"
        
        # Normalize type_name from cursor to SQL datatype_name for creating table
        if dtype == "DECIMAL" or dtype == "DOUBLE" or dtype == "FLOAT":
            columns += row.column_name+" "+dtype+"("+dsize+", "+ddecimal+"), "
        elif dtype ==  "STRING" :
            dtype = "VARCHAR"
            columns += row.column_name+" "+dtype+"("+dsize+"), "
        elif dtype == "DATE" :
            columns += row.column_name+" "+dtype+", "
            
    columns += "flag_date DATE)" # Hacks, confuse how to get last row in table_columns
    sql += columns + " ENGINE=INNODB;"
    # end

    cursor.close
    conn.close
    return schema,tblname,sql

#2 execute sql scripts to .txt file
def exe_query_to_file(conn, sqlquery, o_file, rootdir):
    output_path = os.path.join(rootdir, o_file)
    
    curr_time = dtt.datetime.now().strftime('%Y-%m-%d_%H_%M_%S_%f')[:-3]

    cursor = conn.cursor()
    # start
    result = cursor.execute(sqlquery)

    delim = "|"
    quote = '"'
    f = open(o_file, 'w')
    seq = 0
    seqerr = 0

    header = ""
    for column in cursor.description:
        header += quote + str(column[0]) + quote + delim
    f.write(header + "\n")

    while True:
        seq = seq + 1

        try:
            row = cursor.fetchone()
            # row = row.encode('utf8').strip()
            if row == None:
                break
            # if seq == 2:
            #        break
            line = ""
            for rec in row:
                if str(type(rec)) == "<type 'unicode'>":
                    rec = rec.encode('utf8').strip()

                line += quote + str(rec) + quote + delim

                # line += quote+str(rec.decode('utf-8','ignore').encode("utf-8"))+quote + delim
            # print line
            f.write(line + "\n")
        except Exception as e:
            seqerr = seqerr + 1
            print(type(e))
            print(e.args)
            print("skipping line "+str(seq)+"\n")
            # f.write("skipping line "+str(seq)+"\n")
            # f.write(type(e))
            # f.write(e.args)
            pass

    # end
    result.commit()
    cursor.close
    conn.close

#3 import data txt to local database
def import_file_to_local(tblname,sql):
    pass
#
# ───────────────────────────────────────────────────────────────────── MAIN ─────
#
if __name__ == "__main__":
    pass
    # this script directory
    # rootdir = os.path.dirname(os.path.realpath(__file__))

    # ─── TABLE DALAM BENTUK ARRAY DAN DIIKUTI SCHEMA ───────────────────────────────────────────────────
    # Cth :
    # tblnames = ['schemaname.tablename1', 'schemaname.tablename2']
    # tblnames = ['ar_v.cb_prepaid_postpaid_201906', 'ar_v.cb_prepaid_postpaid_201907']
    # ────────────────────────────────────────────────────────────────────────────────

    # ─── SPLITING SCHEMANAME DAN TABLENAME ──────────────────────────────────────────
    # for name in tblnames:
    #     sp = name.split('.',1)
    #     schema = sp[0]
    #     tblname = sp[1]
    # ────────────────────────────────────────────────────────────────────────────────

    # ─── TEST ───────────────────────────────────────────────────────────────────────
    # schema = 'ar_v'
    # tblname = 'cb_prepaid_postpaid_201906'
    # columns = '*'
    # # Cth var filters : filters = 'WHILE lower(region_lacci) LIKE '%sumbag%'
    # filters = 'LIMIT 10'

    # sqlquery = "SELECT "+columns+" FROM "+schema+"."+tblname+" "+filters+";"
    # sqlquery = re.sub("\s\s+", " ", sqlquery)

    # hdp_conn = hdp_connect()
    # loc_conn = local_connect()
    
    # exe_query_to_file(hdp_conn, sqlquery, 'o_file_test')