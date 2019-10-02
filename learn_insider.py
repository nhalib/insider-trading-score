#parent function evaluating learning of insider autocorrelation
from req_py_libraries import *
from pyArango.connection import *
from learn_insider_cumul import insider_cumul_routine
from learn_insider_baseline import insider_bl_routine,extract_filings
from calc_insider_heat import update_insider_heat

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)

def record_data(coll,code,date_list,heat_list):
    boba = len(date_list)
    ticker = 'suid_'+str(code)
    try:
        doc = coll[ticker]
        for i in range(0,boba):
            doc[date_list[i]] = heat_list[i]
        doc.save()
    except:
        doc = coll.createDocument()
        doc._key = ticker
        for i in range(0,boba):
            doc[date_list[i]] = heat_list[i]
        doc.save()

def process_basic_routine(ticker='NVDA',code=111):
    date_today = datetime.now().date()
    extract_filings(date_today=date_today,days_behind=1800,ticker=ticker)
    date_list = []
    heat_list = []

    offset = 90
    if True:
        for i in range(0,15):
            date_today = date_today - timedelta(days=offset)
            insider_bl_routine(date_today=date_today) # date_today-90 to date_today-270
            insider_cumul_routine(date_today=date_today) # date_today to date_today- 90
            heat = update_insider_heat(ticker = ticker)
            date_list.append(str(date_today))
            heat_list.append(str(heat))
        conn = Connection(username='root', password='Vasily_1992')
        db = conn['learn_params']
        coll = db['insider_heat_history']
        record_data(coll,code,date_list,heat_list)

def process_routine(ticker='NVDA',code=111):
    date_today = datetime.now().date()
    extract_filings(date_today=date_today,days_behind=365,ticker=ticker)
    date_list = []
    heat_list = []
    if True:
        insider_bl_routine(date_today=date_today) # date_today-90 to date_today-270
        insider_cumul_routine(date_today=date_today) # date_today to date_today- 90
        heat = update_insider_heat(ticker = ticker)
        date_list.append(str(date_today))
        heat_list.append(str(heat))
        conn = Connection(username='root', password='Vasily_1992')
        db = conn['learn_params']
        coll = db['insider_heat_history']
        record_data(coll,code,date_list,heat_list)


def setup_insider_heat():
    db,cursor = set_conn('stocks_usa')
    cursor.execute('SELECT ticker,UID from exch_tick_cik WHERE sector LIKE "%semiconductor%"')
    for val in cursor:
        #process_routine(val[0],val[1]) # most recent
        process_basic_routine(val[0],val[1]) # the previous versions

def quarterly_insider_heat():
    db,cursor = set_conn('stocks_usa')
    cursor.execute('SELECT ticker,UID from exch_tick_cik WHERE sector LIKE "%semiconductor%" ')
    for val in cursor:
        process_routine(val[0],val[1])


setup_insider_heat()
