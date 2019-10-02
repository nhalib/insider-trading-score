#author : nikhil.n
# version 1.0
# parse out the Form 4 details

from req_py_libraries import *
from d_insider_be0 import heat_base,heat_base_cumul,heat_base_catchup,set_monthly

def url_response(url):
    boolman = False
    try:
        response = requests.get(url)
        response = response.content.decode('utf-8')
        boolman = True
    except ValueError:
        response = 'Error 101.Page not readable'
    return [boolman, response]

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)


def f4parser(link,cursorn,type,date_today =  datetime.now().date()):
    url_resp = url_response(link)
    data = xmltodict.parse(url_resp[1])
    data = data['ownershipDocument']
    cik = str(int(data['issuer']['issuerCik']))
    cmnd = "SELECT exchange,ticker,UID FROM exch_tick_cik WHERE CIK = {0}".format(cik)
    cursorn.execute(cmnd)
    var = cursorn.fetchall()
    try:
        var = var[0]
        suid = int(var[2])
        var = True
    except:
        print(cik,'doesnt exist in records')
        var = False
    if var:
        owner_pos = []
        try:
             owner_deets = data['reportingOwner']
        except:
            owner_deets = 'NA'
        try:
            owner_cik = owner_deets['reportingOwnerId']['rptOwnerCik']
        except:
            owner_cik = "NA"
        try:
            owner_name = owner_deets['reportingOwnerId']['rptOwnerName']
        except:
            owner_name = "NA"
        try:
            owner_pos.append(owner_deets['reportingOwnerRelationship']['isDirector'])
        except:
            owner_pos.append('0')
        try:
            owner_pos.append(owner_deets['reportingOwnerRelationship']['isOfficer'])
        except:
            owner_pos.append('0')
        try:
            owner_pos.append(owner_deets['reportingOwnerRelationship']['isTenPercentOwner'])
        except:
            owner_pos.append('0')
        try:
            owner_pos.append(owner_deets['reportingOwnerRelationship']['isOther'])
        except:
            owner_pos.append('0')

        owner_profile = ('_').join(x for x in owner_pos)
        reporting_date = data['periodOfReport']
        var = True
        inter_var = True
        v1 = []
        v2 = []
        v3 = []
        try:
            nd = data['nonDerivativeTable']
            nd_trans = nd['nonDerivativeTransaction']
            for trans in nd_trans:
                trans_amnt = trans['transactionAmounts']
                post_trans_amnt = trans['postTransactionAmounts']
                trans_code = trans['transactionCoding']['transactionCode']
                trans_shares = trans_amnt['transactionShares']['value']
                acq_disp = trans_amnt['transactionAcquiredDisposedCode']['value']
                post_trans_shares = post_trans_amnt['sharesOwnedFollowingTransaction']['value']
                v1.append(trans_shares)
                v2.append(post_trans_shares)
                v3.append(acq_disp)
            if type == 1:
                heat_base(suid,reporting_date,owner_name.lower(),v1,v2,v3,date_today)
            elif type ==2:
                heat_base_cumul(suid, reporting_date, owner_name.lower(), v1, v2, v3,date_today)
            elif type == 3:
                heat_base_catchup(suid, reporting_date, owner_name.lower(), v1, v2, v3,date_today)

        except:
            inter_var = False

        if not inter_var:
            try:
                nd = data['nonDerivativeTable']
                trans = nd['nonDerivativeTransaction']
                trans_amnt = trans['transactionAmounts']
                post_trans_amnt = trans['postTransactionAmounts']
                trans_code = trans['transactionCoding']['transactionCode']
                trans_shares = trans_amnt['transactionShares']['value']
                post_trans_shares = post_trans_amnt['sharesOwnedFollowingTransaction']['value']
                acq_disp = trans_amnt['transactionAcquiredDisposedCode']['value']
                if acq_disp == 'A':
                    pass
                elif acq_disp == 'D':
                    pass
                cmnd = "INSERT INTO insider_trading (Owner_CIK,Owner_Name,Owner_Profile,reportingdate,transaction_shares,transaction_code,acq_disp,post_transaction_shares) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

                v1.append(trans_shares)
                v2.append(post_trans_shares)
                v3.append(acq_disp)
                if type ==1:
                    heat_base(suid,reporting_date,owner_name.lower(), v1, v2, v3,date_today)
                elif type ==2:
                    heat_base_cumul(suid, reporting_date, owner_name.lower(), v1, v2, v3,date_today)
                elif type ==3:
                    heat_base_catchup(suid, reporting_date, owner_name.lower(), v1, v2, v3,date_today)
            except:
                if type == 2:
                    heat_base_cumul(suid, reporting_date, owner_name.lower(), v1, v2, v3, date_today)
                elif type ==3:
                    heat_base_catchup(suid, reporting_date, owner_name.lower(), v1, v2, v3,date_today)

                var = False

    return(var,data)

def f4_ndparser(data):
    try:
        d = data['derivativeTable']
        trans = d['derivativeTransaction']
        deriv_flag = True
    except:
        deriv_flag = False
    return(deriv_flag)

def main_f4(type=1):
    date_today = datetime.now().date()
    deriv_flag = False
    db1,cursor1 = set_conn('to_process')
    db2,cursor2 = set_conn('to_process')
    db3,cursor3 = set_conn('stocks_usa')
    cursor1.execute('SELECT UID,link,stat FROM process_f4 WHERE stat = 0')
    for val in cursor1:
        parse_stat,data = f4parser(val[1],cursor3,type)
        if parse_stat:
            cursor2.execute('UPDATE process_f4 SET stat = 1 WHERE UID = {0}'.format(val[0]))
            db2.commit()
        if not parse_stat:
            deriv_flag = f4_ndparser(data)
        if deriv_flag:
            cursor2.execute('UPDATE process_f4 SET stat = 1 WHERE UID = {0}'.format(val[0]))
            db2.commit()

    db1.close()
    db2.close()
    db3.close()
    try:
        set_monthly()
    except:
        pass