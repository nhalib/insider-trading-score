from req_py_libraries import *

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)

def insider_heat_daily(suid,owner_name,frac,diff,date_r):
    db, cursor = set_conn('heats')
    cmnd = 'SELECT UID,itrade_vector,latest_itrade_day FROM insider_heat WHERE insider LIKE "%{0}%" AND stock_uid = {1}'.format(str(owner_name),suid)
    cursor.execute(cmnd)
    rslt = cursor.fetchall()


    if len(rslt) ==1:
        rslt = rslt[0]
        cont_flag = True
        if rslt[1] == None:
            dic = []
            for i in range(0,90):
                dic.append('')
            dic = (":").join(x for x in dic)
            cmnd = 'UPDATE insider_heat SET itrade_vector = "{0}", latest_itrade_day = "{1}" WHERE UID = {2}'.format(str(dic),str(date_r),rslt[0])
            cursor.execute(cmnd)
            db.commit()

            #for the next part
            cont_flag = True
            cmnd = 'SELECT UID,itrade_vector,latest_itrade_day FROM insider_heat WHERE insider LIKE "%{0}%" AND stock_uid = {1}'.format(
                str(owner_name), suid)
            cursor.execute(cmnd)
            rslt = cursor.fetchall()
            rslt = rslt[0] #setup for cont_flag = TRUE

        if cont_flag:

            itrade_dict = rslt[1].split(':')
            try:
                itrade_dict[89-diff] = format(float(itrade_dict[89-diff] +float(frac)),'.2f')
            except:
                itrade_dict[89-diff] = format(float(frac),'.2f')
            itrade_dict = (":").join(str(x) for x in itrade_dict)
            cmnd = 'UPDATE insider_heat SET itrade_vector = "{0}" WHERE UID = {1}'.format(str(itrade_dict), rslt[0])
            cursor.execute(cmnd)
            db.commit()

            try:
                latest_date = datetime.strptime(rslt[2], '%Y-%m-%d').date()
            except:
                latest_date = rslt[2]
            if latest_date < date_r:
                cmnd = 'UPDATE insider_heat SET latest_itrade_day = "{0}" WHERE UID = {1}'.format(str(date_r), rslt[0])
                cursor.execute(cmnd)
                db.commit()