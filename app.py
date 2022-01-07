from flask import Flask, render_template, request
from calculation import accessuser,valueChecker
from databaseConnect import connectdb
import psycopg2
import pandas as pd

app = Flask(__name__)

conn = None
cursor = None

global _nameList
userList = accessuser()
_nameList= [j[1] for  i, j in enumerate (userList)]
df_userList = pd.DataFrame(userList,columns=('userid','username'))
data_headings = ('Id','Name','Month','Outstanding Debt','Share Amount','Loan Installment',
                'Interest Amount','Cash Collected','Debit Balance','New Loan Amount',
                'Total Outstanding Debt')
userNameList = ['Dilip Tripathi','Dinbandhu Tailor','Sunil Joshi','Chandan Singh',
                'Kamal Rawal','Lalit Samdani','Lalit Gurjur','Mukesh Samdani',
                'Mahesh Sharma','Naresh Tailor','Ram Purohit','Rajesh Tailor',
                'Dinesh Joshi','Subash Moghe','Suresh Tailor']

@app.route("/", methods=['GET','POST'])
def input():
    global inputdata
    if request.method=='GET':
        return render_template("home.html",_nameList=_nameList)
    elif request.method=='POST' :
        if request.form["action"] == 'Submit':
            inputdata = request.form
            inDictVal = inputdata.to_dict(flat=True)
            ValList = inDictVal.values()
            inValList = list(ValList)
            inValList = inValList[:-1]
            id = [j[0] for  i, j in enumerate (userList) if j[1] == inValList[0]][0]
            newInValList = [id, inValList[1],int(inValList[2]),int(inValList[3]),int(inValList[4])]
            insertedValue = valueChecker(newInValList)
            #to fetch the name of the user from user table to show the data by name
            #joining with mcb table and user based on user_id
            conn = connectdb()
            cursor = conn.cursor()
            cursor.execute(f"""SELECT *
                            FROM   
                                monthlycontractbalance
                            WHERE  
                                user_id = {id}
                            ORDER  BY update_datetime DESC
                            LIMIT  1 """)
            mcb_data = cursor.fetchall()
            df_mcbData = pd.DataFrame(mcb_data,columns=('user_id','mcb_datemonth','outstanding_debt',
                                                        'share_amount','loan_installment','interest_amount',
                                                        'cash_collected','debit_balance','new_loan_amount',
                                                        'total_outstanding_debt','update_datetime'))
            df_name_mcb = pd.merge(df_userList,df_mcbData, how ='inner',left_on='userid', right_on ='user_id')
            df_name_mcb = df_name_mcb[['user_id','username','mcb_datemonth','outstanding_debt','share_amount','loan_installment',
                            'interest_amount','cash_collected','debit_balance','new_loan_amount','total_outstanding_debt']]
            df_name_mcb = df_name_mcb[:].values
            return render_template("insertValue.html",data_headings=data_headings,df_name_mcb=df_name_mcb)

@app.route("/historicalData",methods=['GET','POST'])
def historicalData():
    global username_value, historysearchdate
    conn = connectdb()
    cursor = conn.cursor()
    if request.method=='GET':
        cursor.execute(f"""SELECT 
                                user_id,
                                username,
                                mcb_datemonth,
                                outstanding_debt,
                                share_amount,
                                loan_installment,
                                interest_amount,
                                cash_collected,
                                debit_balance,
                                new_loan_amount,
                                total_outstanding_debt
                                FROM   
                                    monthlycontractbalance join user on user.userid =  monthlycontractbalance.user_id
                                ORDER  BY user_id, mcb_datemonth
                                """)
        historical_data = cursor.fetchall()
        df_historicalData = pd.DataFrame(historical_data,columns=('user_id','username','mcb_datemonth','outstanding_debt',
                                                            'share_amount','loan_installment','interest_amount',
                                                            'cash_collected','debit_balance','new_loan_amount',
                                                            'total_outstanding_debt'))
        df_historicalData = df_historicalData[:].values
        return render_template("historicaldata.html",userNameList=userNameList,data_headings=data_headings,df_historicalData=df_historicalData)
    elif request.method=='POST':
        if [i for i in userNameList if i == request.form["action"]]:
            username_value = request.form
            fetch_userrval = username_value.to_dict(flat=True)
            inuserval = next(iter(fetch_userrval.values()))

            '''
                fetching user id for specfic user to search or see the
                historical data
            '''
            cursor.execute(f""" SELECT userid from user where username = '{inuserval}' """)
            _fetchId = cursor.fetchall()[0][0] #to fetch only id used indexing
            print("_fetchId:: ",_fetchId)
            cursor.execute(f"""SELECT 
                                user_id,
                                username,
                                mcb_datemonth,
                                outstanding_debt,
                                share_amount,
                                loan_installment,
                                interest_amount,
                                cash_collected,
                                debit_balance,
                                new_loan_amount,
                                total_outstanding_debt
                                FROM   
                                    monthlycontractbalance join user on user.userid =  monthlycontractbalance.user_id
                                WHERE monthlycontractbalance.user_id = {_fetchId}
                                ORDER  BY user_id, mcb_datemonth
                                """)
            user_historicaldata = cursor.fetchall()
            df_historicalData = pd.DataFrame(user_historicaldata,columns=('user_id','username','mcb_datemonth','outstanding_debt',
                                                            'share_amount','loan_installment','interest_amount',
                                                            'cash_collected','debit_balance','new_loan_amount',
                                                            'total_outstanding_debt'))
            df_historicalData = df_historicalData[:].values
            return render_template("historicaldata.html",userNameList=userNameList,data_headings=data_headings,df_historicalData=df_historicalData)
        elif request.form["action" ] == "OK":
            historysearchdate = request.form
            inDictVal = historysearchdate.to_dict(flat=True)
            ValList = inDictVal.values()
            inValList = list(ValList)
            historydateFromSearch = inValList[0]
            hist_dateTo_fetchqry = f"""
                                        SELECT 
                                            user_id,
                                            username,
                                            mcb_datemonth,
                                            outstanding_debt,
                                            share_amount,
                                            loan_installment,
                                            interest_amount,
                                            cash_collected,
                                            debit_balance,
                                            new_loan_amount,
                                            total_outstanding_debt
                                        FROM   
                                            monthlycontractbalance join user on user.userid =  monthlycontractbalance.user_id
                                        WHERE monthlycontractbalance.mcb_datemonth = '{historydateFromSearch}'
                                        ORDER  BY user_id, mcb_datemonth
                                    """
            cursor.execute(hist_dateTo_fetchqry)
            user_historical_datedata = cursor.fetchall()
            df_historical_dateData = pd.DataFrame(user_historical_datedata,columns=('user_id','username','mcb_datemonth','outstanding_debt',
                                                            'share_amount','loan_installment','interest_amount',
                                                            'cash_collected','debit_balance','new_loan_amount',
                                                            'total_outstanding_debt'))
            df_historicalData = df_historical_dateData[:].values
            return render_template("historicaldata.html",userNameList=userNameList,data_headings=data_headings,df_historicalData=df_historicalData)
        else:
            print("bad request!")
    else:
        return "bad request, 404 error!"

@app.route("/updaterecent",methods=['GET','POST'])
def updaterecent():
    global updateinputdata
    conn = connectdb()
    cursor = conn.cursor()
    if request.method=='GET':
        return render_template("updaterecent_home.html",_nameList=_nameList)
    elif request.method=='POST':
        if request.form["action"] == 'Update':
            updateinputdata = request.form
            inDictVal = updateinputdata.to_dict(flat=True)
            ValList = inDictVal.values()
            inValList = list(ValList)
            inValList = inValList[:-1]
            _updateuserid = [j[0] for  i, j in enumerate (userList) if j[1] == inValList[0]][0]
            _updatemonthdate = inValList[1]
            _updateloanInst =  int(inValList[2])
            _updatenewloanamt = int(inValList[3])
            '''
                ***do not change the order of the below "_fetchToupdateqry"
                SELECT query, because based on the order of column we have 
                fetched the parameter and used in calculation
            '''
            _fetchToupdateqry = f"""SELECT
                                        outstanding_debt,
                                        share_amount,
                                        interest_amount
                                    FROM monthlycontractbalance
                                    WHERE
                                        monthlycontractbalance.user_id = {_updateuserid}
                                        AND
                                        monthlycontractbalance.mcb_datemonth = '{_updatemonthdate}'
                                """
            cursor.execute(_fetchToupdateqry)
            data_fetchupdateqry = cursor.fetchall()
            '''
                sql select qry: data return in the format  of type list: [(0, 200, 0)]
            '''
            _updatecashcollected = int(data_fetchupdateqry[0][1]+_updateloanInst+data_fetchupdateqry[0][2])
            _updatedebitbalance = int(data_fetchupdateqry[0][0] - _updateloanInst)
            _updatetotalOutstandingDebt = int(_updatedebitbalance + _updatenewloanamt)
            print("_updatecashcollected: ",_updatecashcollected)
            print("_updatedebitbalance: ",_updatedebitbalance)
            print("_updatetotalOutstandingDebt: ",_updatetotalOutstandingDebt)
            print("_updatemonthdate: ",_updatemonthdate)
            print("Type_updatemonthdate: ",type(_updatemonthdate))
            print("Type_updatecashcollected: ",type(_updatecashcollected))
            print("Type_updatedebitbalance: ",type(_updatedebitbalance))
            print("Type_updatetotalOutstandingDebt: ",type(_updatetotalOutstandingDebt))

            updaterecentqry= f"""
                                UPDATE 
                                    monthlycontractbalance
                                SET 
                                    mcb_datemonth = '{_updatemonthdate}',
                                    loan_installment = {_updateloanInst},
                                    cash_collected = {_updatecashcollected},
                                    debit_balance = {_updatedebitbalance},
                                    new_loan_amount = {_updatenewloanamt},
                                    total_outstanding_debt = {_updatetotalOutstandingDebt}
                                WHERE 
                                    user_id = {_updateuserid} 
                                    AND
                                    mcb_datemonth = '{_updatemonthdate}'
                            """
            cursor.execute(updaterecentqry)
            conn.commit()
            data_update = cursor.fetchall()
            fetchupdatedrecord = (f""" SELECT 
                                            user.username,monthlycontractbalance.* 
                                        FROM 
                                            monthlycontractbalance"
                                        JOIN user
                                        ON user.userid = monthlycontractbalance.user_id 
                                        WHERE 
                                            monthlycontractbalance.user_id = '{_updateuserid}'
                                            AND
                                            monthlycontractbalance.mcb_datemonth = '{_updatemonthdate}'
                                    """)
            cursor.execute(fetchupdatedrecord)
            fetchupdatedrecord_value = cursor.fetchall()
            df_updatedvaluelData = pd.DataFrame(fetchupdatedrecord_value,columns=('username','user_id','mcb_datemonth','outstanding_debt',
                                                            'share_amount','loan_installment','interest_amount',
                                                            'cash_collected','debit_balance','new_loan_amount',
                                                            'total_outstanding_debt','update_datetime'))
            #order of column
            df_updatedvaluelData = df_updatedvaluelData[['user_id','username','mcb_datemonth','outstanding_debt',
                                                            'share_amount','loan_installment','interest_amount',
                                                            'cash_collected','debit_balance','new_loan_amount',
                                                            'total_outstanding_debt']]
            df_updatedvaluelData = df_updatedvaluelData[:].values
            return render_template("updatedrecentvalue.html",data_headings=data_headings,df_updatedvaluelData=df_updatedvaluelData)
        else:
            print("bad error!")
    else:
        print("404 not found error!")

@app.route("/bank",methods=['GET','POST'])
def bank():
    global searchdate
    conn = connectdb()
    cursor = conn.cursor()
    if request.method=='GET':
        fetch_samitibank = """
                                SELECT 
                                    sb_datemonth,
                                    total_cashcollected,
                                    total_newloanamt,
                                    carry_balance,
                                    to_caryfwd_balance,
                                    cashier_amt
                                FROM
                                    samitibank
                            """
        cursor.execute(fetch_samitibank)
        data  = cursor.fetchall()
        return render_template("samitibank_table.html",data=data)
    elif request.method=='POST':
        if request.form["action"] == 'OK':
            searchdate = request.form
            inDictVal = searchdate.to_dict(flat=True)
            ValList = inDictVal.values()
            inValList = list(ValList)
            dateFromSearch = inValList[0]
            _fetch_samitibank = f"""
                                    SELECT 
                                        sb_datemonth,
                                        total_cashcollected,
                                        total_newloanamt,
                                        carry_balance,
                                        to_caryfwd_balance,
                                        cashier_amt
                                    FROM
                                        samitibank
                                    WHERE sb_datemonth >= '{dateFromSearch}'
                                """
            cursor.execute(_fetch_samitibank)
            data  = cursor.fetchall()
            return render_template("samitibank_table.html",data=data)
        else:
            print("bad input!")
    else:
        print("Page Not Found, 404 error!")

@app.route("/deleterecord",methods=["GET","POST"])
def deleterecord():
    global deleteparam
    conn=connectdb()
    cursor = conn.cursor()
    if request.method == 'GET':
        return render_template("deleterecords_template.html",_nameList=_nameList)
    elif request.method == 'POST':
        if request.form["action"] == 'Submit':
            inputdata = request.form
            inDictVal = inputdata.to_dict(flat=True)
            ValList = inDictVal.values()
            inValList = list(ValList)
            print("1--inValList: ",inValList)
            inValList = inValList[:-1]
            print("inValList: ",inValList)
            delete_username = inValList[0]
            delete_datemonth = inValList[1]
            delete_userid = [j[0] for  i, j in enumerate (userList) if j[1] == inValList[0]][0]
            deleteqry = f"""
                            DELETE
                            FROM
                                monthlycontractbalance
                            WHERE
                                user_id = {delete_userid}
                                AND
                                mcb_datemonth >= '{delete_datemonth}'
                        """
            cursor.execute(deleteqry)
            conn.commit()
            return render_template("afterdelete_view.html",delete_username=delete_username,delete_datemonth=delete_datemonth)
        else:
            print("bad request!")
    else:
        print("pagfe not found, 404 error!")

if __name__ == '__main__':
    app.run(debug=True)
