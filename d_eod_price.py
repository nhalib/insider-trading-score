#temporary source end of day stock prices with closing , opening values and volume traded

from req_py_libraries import *
from pyArango.connection import *
import json

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)

def to_date_format(date):
    rslt = datetime.strptime(date, "%Y-%m-%d").date()
    return('"'+str(rslt)+'"')

def eod_to_db(cursor,dat,uid):
    cmnd = 'INSERT INTO {0} (adj_open,adj_high,adj_low,adj_close,adj_volume,date) VALUES ({1},{2},{3},{4},{5},{6})'\
        .format('s_'+str(uid),dat[1],dat[2],dat[3],dat[4],dat[5],to_date_format(dat[0]))
    cursor.execute(cmnd)

def link_fetch(ticker,cursor,tdate,uid):

    link= "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="+str(ticker)+"&apikey=SZ16O6Z3YCUNHJWM&outputsize=full&datatype=csv"
    resp = requests.get(link)
    decoded_content = resp.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    data_list = list(cr)
    data_list = data_list[1:]
    for row in data_list:
        dat = []
        eod_date = datetime.strptime(row[0], "%Y-%m-%d").date()
        cutoff =  datetime.strptime(tdate, "%Y-%m-%d").date()
        if eod_date >= cutoff:
            dat.append(row[0])
            dat.append(row[1])
            dat.append(row[2])
            dat.append(row[3])
            dat.append(row[4])
            dat.append(row[5])
            eod_to_db(cursor,dat,uid)
        else: # assuming eod is sorted by date in descending order
            break


def stock_eod(uid,ticker,exchange,type,tdate = '2014-01-01'):
    db_name = "eod_data"
    db, cursor = set_conn(db_name)

    try:
        cmnd = "CREATE TABLE {0} (UID INT NOT NULL AUTO_INCREMENT, adj_open TEXT, adj_high TEXT, adj_low TEXT, adj_close TEXT, adj_volume TEXT, date DATE, PRIMARY KEY (UID))".format('s_'+str(uid))
        cursor.execute(cmnd)
    except:
        cursor.execute('TRUNCATE TABLE {0}'.format('s_'+str(uid)))
    link_fetch(ticker,cursor,type,tdate,uid)
    db.commit()
    db.close()




def cumu_routine():

    db,cursor = set_conn('stocks_usa')
    cursor.execute('SELECT UID,exchange,ticker FROM exch_tick_cik WHERE sector LIKE "%semiconductor%"')
    for val in cursor:
            stock_eod(uid=val[0],ticker=val[2],exchange=val[1])
            print(val[0])


cumu_routine()