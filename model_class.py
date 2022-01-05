from numpy.core.numeric import outer
from databaseConnect import connectdb
import mysql.connector as mysql
import pandas as pd

class Fetchdataset:
    def __init__(self,createcursor):
        try:
            self.cursor = createcursor
        except:
            print("issue in invocation of class Fetchdataset")
    
    def qryMCBTable(self):
        try:
            self.dataqry = """
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
                            FROM monthlycontractbalance
                            JOIN user
                            ON monthlycontractbalance.user_id = user.userid    
                            """
            self.cursor.execute(self.dataqry)
            # df.loc[df['A'] == 'foo']   --> complete row value
            df_mcbData = pd.DataFrame(self.cursor.fetchall(), columns=self.cursor.column_names)
            # print("Type_df_mcbData: ",type(df_mcbData))
            # print("df_mcbData: ",df_mcbData.count())
            return df_mcbData
        except:
            print("issue in data qry")
            raise

class Calculation:
    def __init__(self,createcursor):
        try:
            self.cursor = createcursor
            print("object invocation of fetchdtaset")
            Fetchdataset.__init__(self,createcursor)
        except:
            print("issue in Calculation class")
            raise
    def callDataFrame(self):
        try:
            df_qrydata = Fetchdataset.qryMCBTable(self)
            return df_qrydata
        except:
            print("issue in callDataFrame")
            raise

if __name__ == '__main__':
    #create session for database
    conn = connectdb()
    createcursor = conn.cursor()
    
    #class invocation
    # modelObj = Fetchdataset(createcursor)
    # outputSet = modelObj.qryMCBTable()
    CalcObj = Calculation(createcursor)
    outputSet = CalcObj.callDataFrame()
    print("outputSet: ",outputSet.count())
    print("TypeoutputSet: ",type(outputSet))
    # writer = pd.ExcelWriter('/home/deepu/Desktop/SamitiMandal/outputSet.xlsx')      
    # outputSet.to_excel(writer)
    # writer.save()
    # print("excel done")
