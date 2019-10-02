#time-period defined update of baseline of insider trading pattern

from sec_data_class import sec_data
from req_py_libraries import *
from process_f4_data import f4parser,f4_ndparser

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)


#dateb : filings should be before this
#cutoff : filings should be after this
def form4_scrape_routine(class_var,cik,cutoff,dateb,cursor,db):
    super_flag = True
    ll = 0
    ul = 100
    while super_flag:
        link = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="
        l3 = "&type="
        form = "4%25"
        l4 = "&dateb=&owner=include&"
        l5 = "start="
        l6 = str(ll)
        l7 = "&count="
        l8 = str(ul)
        l9 = "&output=atom"

        link = link + cik + l3 + form + l4 + l5 + l6 + l7 + l8 + l9
        datec = datetime.strptime(cutoff, "%Y-%m-%d").date()
        feed = feedparser.parse(link)
        feed = feed['entries']
        for i in range(0,len(feed)):
                datef = feed[i]['filing-date']
                datef = datetime.strptime(datef, "%Y-%m-%d").date()
                ftype = re.compile(feed[i]['filing-type'])
                flink = feed[i]['filing-href']
                if(datef>=datec and ftype.match('4') and datef<=dateb):
                    class_var.process_F4_insider(datef,flink, cursor)
                elif (datef<datec and ftype.match('4')):
                    super_flag = False
                    break
        if len(feed) == 0:
            super_flag = False
        if ll%500 == 0:
            db.commit()
        ll = ll + 100


def extract_filings(date_today=datetime.now().date(),days_behind=360,ticker='NVDA'):
    x = sec_data()
    [db, cursor] = set_conn(db_name='stocks_usa')
    [db2, cursor2] = set_conn(db_name='to_process')
    cursor2.execute('TRUNCATE process_f4_insider_learn')
    db2.commit()
    cmnd = "SELECT exchange,ticker,cik FROM exch_tick_cik WHERE ticker = '{0}'".format(str(ticker))
    cursor.execute(cmnd)
    cutoff = str(date_today - timedelta(days=days_behind))
    for val in cursor:
        cik = val[2]
        form4_scrape_routine(x, cik, cutoff, date_today, cursor2,db2)
    db2.commit()
    db.close()
    db2.close()


def insider_bl_routine(date_today=datetime.now().date()):
    deriv_flag = False
    db1, cursor1 = set_conn('to_process')
    db2, cursor2 = set_conn('to_process')
    db3, cursor3 = set_conn('stocks_usa')
    cursor3.execute('UPDATE heats.insider_heat SET baseline_heat = NULL ,baseline_samples = NULL')
    db3.commit()
    date_ub = str(date_today)
    date_lb = str(date_today - timedelta(days=360))
    cmnd = 'SELECT UID,link,stat FROM process_f4_insider_learn WHERE stat = 0 AND file_date >= "{0}" AND file_date <= "{1}"'.format(date_lb,date_ub)
    cursor1.execute(cmnd)
    for val in cursor1:
        parse_stat, data = f4parser(val[1], cursor3, 3,date_today)
        if parse_stat:
            cursor2.execute('UPDATE process_f4_insider_learn SET stat = 0 WHERE UID = {0}'.format(val[0]))
            db2.commit()
        if not parse_stat:
            deriv_flag = f4_ndparser(data)
        if deriv_flag:
            cursor2.execute('UPDATE process_f4_insider_learn SET stat = 10 WHERE UID = {0}'.format(val[0]))
            db2.commit()

    db1.close()
    db2.close()
    db3.close()