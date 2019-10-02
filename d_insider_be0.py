from d_insider_be1 import *


def generate_frac(traded_amnts,aftertraded_amnts,traded_types):
    frac = 0
    write_flag = True
    j = 0
    for traded_amnt in traded_amnts:
            traded_type = traded_types[j]
            if traded_type == "A":
                v0 = float(traded_amnt)
                v1 = float(aftertraded_amnts[j]) - v0
                if v1 == 0:
                    if v0 == 0:
                        temp_frac = 0
                    else:
                        temp_frac = 1
                else:
                    temp_frac = v0/v1

                frac = frac + min(temp_frac,1)
                if v1 == 0:
                    write_flag = False
            if traded_type == "D":
                v0 = float(traded_amnt)
                v1 = float(aftertraded_amnts[j]) + v0
                temp_frac = v0 / v1
                frac = frac -  temp_frac
                if v1 ==0:
                    write_flag = False
            j = j+1
    return(frac,write_flag)




def insider_ma(suid,owner_name,frac,date_r,type,date_today):
    db,cursor = set_conn('heats')
    cmnd = 'SELECT UID,baseline_heat,baseline_samples,latest_itrade_day FROM insider_heat WHERE insider LIKE "%{0}%" AND stock_uid = {1}'.format(str(owner_name),suid)
    cursor.execute(cmnd)
    rslt = cursor.fetchall()
    if len(rslt) == 0:
        frac = format(frac,'.2f')
        cmnd = 'INSERT INTO insider_heat (insider,baseline_heat,baseline_samples,stock_uid,latest_itrade_day) VALUES ("{0}","{1}","{2}",{3},"{4}")'.format(owner_name,str(frac),'1',suid,str(date_r))
        cursor.execute(cmnd)
        db.commit()
    elif len(rslt) ==1 and type == 1:
        rslt = rslt[0]
        try:
            old_heat = float(rslt[1])
            old_count = int(rslt[2])
            tempnum = np.multiply(old_heat,old_count)+float(frac)
            tempden = old_count+1
            new_count = old_count + 1
        except:
            tempnum = float(frac)
            tempden = 1
            new_count = 1
        new_heat = format(tempnum/tempden,'.2f')
        try:
            latest_date = datetime.strptime(rslt[3], '%Y-%m-%d').date()
        except:
            latest_date = rslt[3]

        if (date_today - date_r).days <=360:
            cmnd = 'UPDATE insider_heat SET baseline_heat="{0}",baseline_samples="{1}" WHERE UID = {2}'.format(str(new_heat), str(new_count), rslt[0])
            cursor.execute(cmnd)
            db.commit()

            if latest_date  < date_r:
                cmnd = 'UPDATE insider_heat SET latest_itrade_day = "{0}" WHERE UID = {1}'.format(str(date_r),rslt[0])
                cursor.execute(cmnd)
                db.commit()
    db.close()



def set_monthly():
    db, cursor = set_conn('heats')
    db1, cursor1 = set_conn('heats')
    cmnd = 'UPDATE insider_heat SET heat_1 = NULL, heat_2 = NULL, heat_3 = NULL'
    cursor.execute(cmnd)
    db.commit()
    cmnd = 'SELECT UID,itrade_vector FROM insider_heat'
    cursor.execute(cmnd)
    for vals in cursor:
        try:
            temp = vals
            trade_array = temp[1].split(':')
            itrades = []
            for itrade in trade_array:
                try:
                    itrades.append(float(itrade))
                except:
                    itrades.append(0)

            heat_3 = [float(x) for x in itrades[0:30]]
            heat_3 = sum(heat_3)
            cmnd = 'UPDATE insider_heat SET heat_3 = "{0}" WHERE UID = {1}'.format(str(heat_3),temp[0])
            cursor1.execute(cmnd)
            heat_2 = [float(x) for x in itrades[31:60]]
            heat_2 = sum(heat_2)
            cmnd = 'UPDATE insider_heat SET heat_2 = "{0}" WHERE UID = {1}'.format(str(heat_2), temp[0])
            cursor1.execute(cmnd)
            heat_1 = [float(x) for x in itrades[61:89]]
            heat_1 = sum(heat_1)
            cmnd = 'UPDATE insider_heat SET heat_1 = "{0}" WHERE UID = {1}'.format(str(heat_1), temp[0])
            cursor1.execute(cmnd)
            db1.commit()
        except:
            pass
    db.close()
    db1.close()


#to update trading score
def heat_base(suid,date_r,owner_t,traded_amnts,aftertraded_amnts,traded_types,date_today):
    db,cursor = set_conn('heats')
    cmnd = 'SELECT UID,baseline_heat,baseline_samples,latest_itrade_day FROM insider_heat WHERE insider LIKE "%{0}%" AND stock_uid = {1}'.format(str(owner_t),suid)
    cursor.execute(cmnd)
    rslt = cursor.fetchall()
    if len(rslt) == 0:
        cmnd = 'INSERT INTO insider_heat (insider) VALUES ("{0}")'.format(owner_t)
        cursor.execute(cmnd)
        db.commit()
    db.close()

    [frac,write_flag]=generate_frac(traded_amnts,aftertraded_amnts,traded_types)
    if write_flag:
            date_r = datetime.strptime(date_r,'%Y-%m-%d').date()
            if (date_today - date_r).days < 90 and (date_today - date_r).days >= 0:
                insider_heat_daily(suid,owner_t,frac,(date_today - date_r).days,date_r)


def heat_base_cumul(suid,date_r,owner_t,traded_amnts,aftertraded_amnts,traded_types,date_today):
    write_flag = True
    if write_flag:
            date_r = datetime.strptime(date_r,'%Y-%m-%d').date()
            if (date_today - date_r).days < 90 and (date_today - date_r).days >= 0:
                [frac, write_flag] = generate_frac(traded_amnts, aftertraded_amnts, traded_types)
                insider_heat_daily(suid,owner_t,frac,(date_today - date_r).days,date_r)

#to update baseline trading score
def heat_base_catchup(suid,date_r,owner_t,traded_amnts,aftertraded_amnts,traded_types,date_today):
            date_r = datetime.strptime(date_r,'%Y-%m-%d').date()
            if (date_today - date_r).days > 90 and (date_today - date_r).days >= 0:
                [frac, write_flag] = generate_frac(traded_amnts, aftertraded_amnts, traded_types)
                insider_ma(suid,owner_t,frac,date_r,1,date_today)



