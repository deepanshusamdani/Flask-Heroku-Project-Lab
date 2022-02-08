from datetime import date
from numpy.lib.function_base import select
from numpy.lib.shape_base import column_stack
from datetime import datetime
from databaseConnect import connectdb
from decimal import Decimal
import psycopg2
import numpy as np
import pandas as pd

cursor = None
conn = None

def accessuser():
    '''
        return: data in list
    '''
    try:
        conn = connectdb()
        cursor = conn.cursor()
        if conn is not None:
            cursor.execute("SELECT * FROM userobj;")
            Userdata = cursor.fetchall()
            print(Userdata)
            print("type: ",type(Userdata))
            return Userdata
    except:
        print("issue in accessing user")
        raise

def insertMonthlyValues(readParam):
    global conn
    global cursor
    print("inside uMCB")
    try:
        conn = connectdb()
        cursor = conn.cursor()
        
        data_user = accessuser()

        _userId = readParam[0]
        _monthDate = readParam[1]
        _loanInst = readParam[2]
        _newloanAmt = readParam[3]
        _cashierAmt = readParam[4]  #once in year have value 1000RS, (Jan) of every year
        cursor.execute(f"SELECT interestrate from interest where datemonth ='{_monthDate}'")
        _interestRate =  cursor.fetchall()[0][0] #fetching interestrate value
        print("_interestRate_1: ",_interestRate)
        _interestRate = float(_interestRate) * (0.01)
        print("_interestRate_2: ",_interestRate)
        
        if conn is not None:
            mcb_qry= f"""
                        SELECT 
                            user_id,
                            mcb_datemonth,
                            outstanding_debt,
                            share_amount,
                            loan_installment,
                            interest_amount,
                            cash_collected,
                            debit_balance,
                            new_loan_amount,
                            total_outstanding_debt
                        FROM monthlycontractbalance
                        WHERE  
                            user_id = {_userId}
                            AND 
                            mcb_datemonth = (
                                            SELECT Max(mcb_datemonth) 
                                            FROM "monthlycontractbalance"
                                            where user_id = {_userId}
                                            )
                        """
            cursor.execute(mcb_qry)
            # df.loc[df['A'] == 'foo']   --> complete row value
            df_mcbData = pd.DataFrame(cursor.fetchall(), columns=('user_id','mcb_datemonth','outstanding_debt',
                                                            'share_amount','loan_installment','interest_amount',
                                                            'cash_collected','debit_balance','new_loan_amount',
                                                            'total_outstanding_debt'))
            print("df_mcbData: ",df_mcbData)
            if [i for i in df_mcbData['user_id'] if i == _userId]: 
                _uId = _userId
                _MonthDate = _monthDate
                if _monthDate in ('2020-04-01','2020-05-01','2020-06-01','2021-05-01','2021-06-01'):
                    '''
                        in these three months(APR,MAY,JUN 2020) "Samiti" was not happened
                        so no shareAmount and loanInstallement has been taken to customers
                        and taken these amounts in the month of July-2020.
                        Since hardcording(H.C,) the values of fields as 0...
                    '''
                    _OutstandingDebts = int(df_mcbData['total_outstanding_debt'][0])
                    _ShareAmount = int(0)   #H.C.
                    _LoanInstallment = int(0) #H.C.
                    _interestAmount = int(_OutstandingDebts*(0))    #H.C.
                    _cashCollected =  _ShareAmount + _LoanInstallment + _interestAmount
                    _debtBalance = int(_OutstandingDebts - _LoanInstallment)
                    _newLoanAmount = int(0) #H.C.
                    _totalOutstandingDebt = int(_debtBalance + _newLoanAmount)
                elif _monthDate == '2020-07-01':
                    '''
                        due to covid after three months, in July-2020, has taken only 
                        sharedAmount of total 4 months i.e. (800) #H.C., and no loanInstallments
                        taken in month of July-2020, hence it's value is 0 (zero) #H.C.
                        and interest rate is 4% due to the 4 months of amount
                        but new loan amount has been released
                    '''
                    _OutstandingDebts = int(df_mcbData['total_outstanding_debt'][0])
                    _ShareAmount = int(800)     #H.C.
                    _LoanInstallment = int(0)   #H.C.
                    _interestAmount = int((np.ceil(_OutstandingDebts*(0.01)))*4)#H.C. 
                    _cashCollected =  _ShareAmount + _LoanInstallment + _interestAmount
                    _debtBalance = int(_OutstandingDebts - _LoanInstallment)
                    _newLoanAmount = _newloanAmt #H.C.
                    _totalOutstandingDebt = int(_debtBalance + _newLoanAmount)
                else:
                    #defalt this should be run always
                    _OutstandingDebts = int(df_mcbData['total_outstanding_debt'][0])
                    _ShareAmount = int(200)
                    _LoanInstallment = _loanInst
                    _interestAmount = int(np.ceil(_OutstandingDebts*(_interestRate)))
                    _cashCollected =  _ShareAmount + _LoanInstallment + _interestAmount
                    _debtBalance = int(_OutstandingDebts - _LoanInstallment)
                    _newLoanAmount = _newloanAmt
                    _totalOutstandingDebt = int(_debtBalance + _newLoanAmount)

                
                print("uID: ",_uId)
                print("Date: ",_MonthDate)
                print("outStdebt: ",_OutstandingDebts)
                print("ShrAmt: ",_ShareAmount)
                print("LA: ",_LoanInstallment)
                print("IAmt: ",_interestAmount)
                print("CColl: ",_cashCollected)
                print("DBal: ",_debtBalance)
                print("neLoan: ",_newLoanAmount)
                print("totOus: ",_totalOutstandingDebt)
                # read_insertValues = [_uId,_MonthDate,_OutstandingDebts,_ShareAmount,_LoanInstallment,
                #                     _interestAmount, _cashCollected,_debtBalance,_newLoanAmount,
                #                     _totalOutstandingDebt]
                # print("RIV----: ",read_insertValues)

                updateDateTimeVal = pd.to_datetime(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                _updateDateTime = datetime.fromisoformat(updateDateTimeVal)
                
                cursor.execute("""
                                INSERT INTO monthlycontractbalance
                                            (user_id,
                                            mcb_datemonth,
                                            outstanding_debt,
                                            share_amount,
                                            loan_installment,
                                            interest_amount,
                                            cash_collected,
                                            debit_balance,
                                            new_loan_amount,
                                            total_outstanding_debt,
                                            update_datetime) 
                                VALUES      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """,
                                (_uId,_MonthDate,_OutstandingDebts,_ShareAmount,
                                _LoanInstallment,_interestAmount,_cashCollected,
                                _debtBalance,_newLoanAmount,_totalOutstandingDebt,_updateDateTime))
                conn.commit()
                '''
                    to get the exact previous month of the entered date
                '''
                qry_exactPreviousMonthDate = f"""
                                                SELECT 
                                                    distinct(last_day('{_monthDate}' - interval 2 month) + interval 1 day)
                                                FROM monthlycontractbalance;
                                            """
                '''
                    carry_balance: previous month amount which remain left after giving loan
                    to_carryfwd_balance: amount remains after provinding loan in current month
                '''
                cursor.execute(qry_exactPreviousMonthDate)
                previousmonthdate = cursor.fetchall()[0][0]
                fetch_samitibank = f"""
                                        SELECT 
                                            to_caryfwd_balance
                                        FROM 
                                            samitibank
                                        where sb_datemonth = '{previousmonthdate}'
                                    """
                cursor.execute(fetch_samitibank)
                _carrybalancePreMon = cursor.fetchall()[0][0]
                
                fetch_mcb = f"""
                                SELECT 
                                    mcb_datemonth,
                                    SUM(cash_collected),
                                    SUM(new_loan_amount)
                                FROM
                                    monthlycontractbalance
                                WHERE 
                                    mcb_datemonth = '{_monthDate}'
                                GROUP BY 
                                    mcb_datemonth
                            """
                cursor.execute(fetch_mcb)
                __fetchvalue = cursor.fetchall()
                
                _fetchmcbDate = __fetchvalue[0][0]
                _fetchmcbTotalCollected = int(str(__fetchvalue[0][1]))  #it gives result in Decimal('x') so need to fetch value as int
                _fetchmcbNewLoan =  int(str(__fetchvalue[0][2]))
                
                _carryfwdamount = (_fetchmcbTotalCollected + _carrybalancePreMon) - _fetchmcbNewLoan
                _cashieramtvalue = _cashierAmt

                '''
                    to check samitibank table, that month entry already there or not
                '''
                cursor.execute("""SELECT distinct sb_datemonth from samitibank""")
                list_existingMonths = cursor.fetchall()
                
                #loop iterate to fetch the exat value of date from given format
                monthList = []
                for i,j in enumerate(list_existingMonths):
                    monthList.append(list_existingMonths[i][0].strftime("%Y-%m-%d"))

                _formatmonthDate = f"'{_monthDate}'"

                def checkIfMatch(mList,_fmonthDate):
                    '''
                        this function is created to check the samitibank table
                        has that date or not... if it have then "True" 
                        so it means we need to update the value of the metrics for that date
                        else "False" means we need to create record for first time for that date
                    '''
                    if _monthDate in mList:
                        return True
                    else :
                        return False

                bool_flag  = checkIfMatch(monthList,_monthDate)
                print("bool_flag: ",bool_flag)

                if bool_flag:
                    '''
                        if samitibank table has already entry for that date
                        then for second time it would update the values
                    '''
                    cursor.execute(f"""
                                    UPDATE 
                                        samitibank
                                    SET
                                        total_cashcollected = {_fetchmcbTotalCollected},
                                        total_newloanamt = {_fetchmcbNewLoan},
                                        carry_balance = {_carrybalancePreMon},
                                        to_caryfwd_balance = {_carryfwdamount},
                                        cashier_amt = {_cashieramtvalue},
                                        created_datetime = '{_updateDateTime}'
                                    WHERE sb_datemonth = {_formatmonthDate}
                                    """)
                    conn.commit()
                    print(f"""
                            UPDATE 
                                samitibank
                            SET
                                total_cashcollected = {_fetchmcbTotalCollected},
                                total_newloanamt = {_fetchmcbNewLoan},
                                carry_balance = {_carrybalancePreMon},
                                to_caryfwd_balance = {_carryfwdamount},
                                cashier_amt = {_cashieramtvalue},
                                created_datetime = '{_updateDateTime}'
                            WHERE sb_datemonth = {_formatmonthDate}
                            """)
                else:
                    '''
                        for first entry of the date in the samitibank table
                        this condition would run to create the record
                    '''
                    cursor.execute("""
                                    INSERT INTO 
                                        samitibank
                                            (
                                                sb_datemonth,
                                                total_cashcollected,
                                                total_newloanamt,
                                                carry_balance,
                                                to_caryfwd_balance,
                                                cashier_amt,
                                                created_datetime
                                            )
                                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                                    """,(_fetchmcbDate,_fetchmcbTotalCollected,_fetchmcbNewLoan,
                                        _carrybalancePreMon,_carryfwdamount,_cashieramtvalue,
                                        _updateDateTime))
                    conn.commit()
                
                #making list clear for every iteration
                monthList.clear()
                return df_mcbData
    except:
        print("issue in updatingMonthlyValues")
        raise

def valueChecker(checkParam):
    try:
        print("inside valueChecker")
        conn = connectdb()
        cursor = conn.cursor()
        check_userID = checkParam[0]
        check_monthDate = checkParam[1]
        #str to datetime/date
        _checkMonthDate = (datetime.fromisoformat(check_monthDate)).date()
        
        fetchMCB = f"""
                        SELECT 
                            max(mcb_datemonth)
                        FROM 
                            monthlycontractbalance
                        where 
                            user_id = {check_userID}
                    """
        cursor.execute(fetchMCB)
        
        MaxdataValue = cursor.fetchall()[0][0]
        user_id_List = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]

        if check_userID in user_id_List:
            if MaxdataValue >= _checkMonthDate:
                print("already have records")
                return
            else:
                #to insert values in table
                print("to insert values in table")
                insertedValue = insertMonthlyValues(checkParam)
            return insertedValue
        else:
            print("Not a valid user ID, re-enter")
            askedUser()
    except:
        print("issue in valueChecker")
        raise

def askedUser():
    try:
        userID_Param = int(input("Enter User ID: "))
        month_Param = input("Enter 1st Date of Current Month like 'YYYY-MM-01': ")
        laonInst_Param = int(input("Enter Loan Installement if ant elese enter 0: "))
        newloan_Param = int(input("Enter New Loan Amount if any else enter 0: "))
        
        inputParam = [userID_Param,month_Param,laonInst_Param,newloan_Param]
        
        #to check entered values are true or not-
        valueChecker(inputParam)        
        return inputParam
    except:
        print("issue in askedUser")
        raise

# if __name__ == '__main__':
    # inputParam = askedUser()
    # mcb_output = updatingMonthlyValues(inputParam)
    # while True:
    #     flag = int(input("Enter 1 if you want to insert new entry else 0: "))
    #     if flag==1:
    #         askedUser()
    #         continue
    #     break
    # a = accessuser()
        
    # print("done")

#fixed table header:
#https://codepen.io/nauerster/pen/emJzyP

# print("uID: ",type(_uId))
# print("Date: ",type(_MonthDate))
# print("outStdebt: ",type(_OutstandingDebts))
# print("ShrAmt: ",type(_ShareAmount))
# print("LA: ",type(_LoanInstallment))
# print("IAmt: ",type(_interestAmount))
# print("CColl: ",type(_cashCollected))
# print("DBal: ",type(_debtBalance))
# print("neLoan: ",type(_newLoanAmount))
# print("totOus: ",type(_totalOutstandingDebt))

# read_insertValues = [_uId,_MonthDate,_OutstandingDebts,_ShareAmount,_LoanInstallment,
#                      _interestAmount, _cashCollected,_debtBalance,_newLoanAmount,
#                     _totalOutstandingDebt]
# # print("RIV----: ",read_insertValues)

#particular vaslue of column based on condition
# OutstandingDebts_df = df_mcbData[df_mcbData['user_id']==_uId]
# _OutstandingDebts = OutstandingDebts_df['total_outstanding_debt'][0]
