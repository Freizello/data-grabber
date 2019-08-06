import os
import datetime as dtt
import pyodbc

import connect #connect.py

def grab_data(qid, filename, query):
    # print(filename)
    # print(query)

    pyodbc.autocommit = True
    pyodbc.pooling = False
    
    conn = connect.local()

    curr_time = dtt.datetime.now().strftime('%Y-%m-%d_%H_%M_%S_%f')[:-3]

    cursor = conn.cursor()
    # start
    print(query)
    
    result = cursor.execute(query)

    delim = "|"
    quote = '"'
    f = open(filename, 'w')
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
            line = line[:-1]
            # print(line)
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


if __name__ == "__main__":
    
    queries = {
        1: {
            "title": "sample_sum1",
            "query": "select region_lacci, cluster_lacci, sum(msisdn) as cb, sum(total_revenue) as rev from nonhvc_test.cb_prepaid_postpaid_201906 group by region_lacci, cluster_lacci",
            "desc": "description_text4"
        }
    }

    for q_id, q_details in queries.items():
        fname = queries[q_id]['title']
        query = queries[q_id]['query']
        desc = queries[q_id]['desc']
        
        grab_data(q_id, fname, query)
