#should have called m_insider_bl before this
#performs a cumulative operation to set up intial itrade vectors for past 3 months

from req_py_libraries import *
from sec_data_class import sec_data
from process_f4_data import f4parser,f4_ndparser,set_monthly

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)


def insider_cumul_routine(date_today=datetime.now().date()):
    deriv_flag = False
    db1, cursor1 = set_conn('to_process')
    db2, cursor2 = set_conn('to_process')
    db3, cursor3 = set_conn('stocks_usa')
    cursor3.execute('UPDATE heats.insider_heat SET itrade_vector = NULL')
    db3.commit()
    date_ub = str(date_today)
    date_lb = str(date_today - timedelta(days=90))
    cmnd = 'SELECT UID,link,stat FROM process_f4_insider_learn WHERE stat = 0 AND file_date >= "{0}" AND file_date <= "{1}"'.format(
        date_lb, date_ub)
    cursor1.execute(cmnd)
    for val in cursor1:
        parse_stat, data = f4parser(val[1], cursor3, 2,date_today)
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
    try:
        set_monthly()
    except:
        pass


