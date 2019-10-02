#parent function evaluating learning of insider autocorrelation
from req_py_libraries import *
from pyArango.connection import *
from scipy.stats.stats import pearsonr
from sklearn import linear_model

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)

def eod_within_bounds(lb,ub,suid):
    db,cursor = set_conn('eod_data')
    stat = True
    close = 0
    cmnd = 'SELECT MAX(date) FROM {0}'.format('s_'+str(suid))
    cursor.execute(cmnd)
    final_d = cursor.fetchone()[0]
    if final_d < ub:
        stat = False
    else:
        cmnd = 'SELECT adj_close FROM {0} WHERE date <= "{1}" AND date >= "{2}" ORDER BY date ASC'.format('s_'+str(suid),str(ub),str(lb))
        cursor.execute(cmnd)
        close = []
        for val in cursor:
            close.append(val[0])
    return [stat,close]

#moving average defined with size as n , time series as series
def calc_ma(n,series):
    size = len(series)
    ma = []
    for i in range(0,size):
        try:
                back = [float(x) for x in series[i-(n-1):i+1]]
                ma.append(sum(back)/n)
        except:
                pass

    return(ma)

#regression fit to get slope
def estimate_slope(y_series):
    x_series = [y+1 for y in range(0,len(y_series))]
    x_series = np.reshape(x_series,(-1,1))
    regr = linear_model.LinearRegression()
    regr.fit(x_series,y_series)
    return regr.coef_

def correlate(series1,series2):
    rslt = pearsonr(series1,series2)
    return rslt

#stock performance (slope of 7-day moving average of stock over the delay period)
def get_stckperf(begin_date,delay,suid):
    lb = datetime.strptime(begin_date, "%Y-%m-%d").date()
    ub = lb + timedelta(days = delay)
    [stat,eod_series] = eod_within_bounds(lb,ub,suid)
    stat2 = False
    slope = 0
    if stat:
        eod_series_smoothed = calc_ma(7,eod_series)
        slope = estimate_slope(eod_series_smoothed)
        stat2 = True
    return [stat2,slope[0]]

#check consisitency of stock
def series_validate(series1,series2):
    pass


#given a stock uid and corresponding delay factor
def correlator_base(suid,delay):
    keys = ['2018-02-09','2017-11-11','2017-08-13','2017-05-15','2017-02-14','2016-11-16','2016-08-18','2016-05-20','2016-02-20'
            ,'2015-11-22','2015-08-24','2015-05-26','2015-02-25','2014-11-27','2014-08-29']
    conn = Connection(username='root', password='Vasily_1992')
    db = conn['learn_params']
    coll = db['insider_heat_history']
    doc = coll['suid_'+str(suid)]
    series1 = []
    series2 = []
    rslt = False
    for key in keys:
        tkey = datetime.strptime(key, "%Y-%m-%d").date() + timedelta(days=1)
        tkey = str(tkey)
        try:
            [stata,perf] =get_stckperf(begin_date=tkey, delay=delay, suid=suid)
            statc = True
        except:
            statc = False
            stata = False
        try:
            float(doc[tkey])
            statb = True

        except:
            statb = False
        if stata and statb and statc:
            series2.append(perf)
            series1.append(float(doc[tkey]))
    if len(series1) >1 and len(series2) > 1:
        rslt = correlate(series1,series2)

    return [stata,rslt]


def correlate_routine(suid):
        shift = 10
        proceed_flag = True
        opti_shift = 0
        opti_cor = 0
        opti_p = 0
        while shift <=365 and proceed_flag:
            [overflow,correlation_stat]=correlator_base(suid,shift)
            if not overflow:
                proceed_flag = False
            elif proceed_flag:
                cor = correlation_stat[0]
                cor_p = correlation_stat[1]
                print(cor, opti_cor,shift)
                if cor > opti_cor:
                    opti_cor = cor
                    opti_p = cor_p
                    opti_shift = shift
            shift = shift + 5
        print(suid,opti_cor, opti_p, opti_shift)

correlate_routine(1492)













