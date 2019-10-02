from req_py_libraries import *

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)

def update_insider_heat(ticker):
    db,cursor = set_conn('stocks_usa')
    db1,cursor1 = set_conn('heats')
    cmnd = 'SELECT UID FROM exch_tick_cik WHERE ticker = "{0}"'.format(str(ticker))
    cursor.execute(cmnd)
    uids = cursor.fetchall()
    month_1 = 0
    c1 = 0
    month_2 = 0
    c2 = 0
    month_3 = 0
    c3 = 0
    heat = 0
    for uid in uids:
        cmnd = 'SELECT baseline_heat,heat_1,heat_2,heat_3 FROM insider_heat WHERE stock_uid = {0}'.format(uid[0])
        cursor1.execute(cmnd)
        rslts = cursor1.fetchall()
        if len(rslts)>0:
            for rslt in rslts:
                if rslt[0] is None:
                    pass
                else:
                    if rslt[1] is None:
                        pass
                    else:
                        if float(rslt[1]) != 0:
                            month_1 = month_1 +float(rslt[1]) - float(rslt[0]) #deviation from baseline
                            c1 = c1 + 1

                    if rslt[2] is None:
                        pass
                    else:
                        if float(rslt[2]) != 0:
                            month_2 = month_2 +float(rslt[2]) - float(rslt[0]) #deviation from baseline
                            c2 = c2 + 1
                    if rslt[3] is None:
                        pass
                    else:
                        if float(rslt[3]) != 0:
                            month_3 = month_3 + float(rslt[3]) - float(rslt[0]) # deviation from baseline
                            c3 = c3 + 1
        pscale = c1**2
        w1 = month_1*pscale*np.exp(-1)
        pscale = c2 ** 2
        w2 = month_2*pscale*np.exp(-2)
        pscale = c3 ** 2
        w3 = month_3*pscale*np.exp(-3)
        heat =  format(w1+w2  +w3,'.2f')
        #cmnd = "UPDATE exch_tick_cik SET insider_heat = '{0}' WHERE UID = {1}".format(str(heat),uid[0])
        #cursor0.execute(cmnd)


    db.close()
    db1.close()
    return heat