import pyodbc
import traceback
import time
from datetime import datetime
from Logger import Logger

class syncjob(object):
    """Carries variables read in from sync files, as well as sync methods."""
    connectionString1=""
    connectionString2=""
    syncInterval=1
    tableMaps=[]

    connection1=None
    connection2=None

    def __init__(self):
        None
    
    def testConnection(self):
        try:
            self.connect()
            self.disconnect()
            return True
        except Exception as e:  
            Logger.writeAndPrintLine("Test connection failed "+traceback.format_exc(),3)   
            return False
    
    def connect(self):
        try:
            self.connection1=pyodbc.connect(self.connectionString1)
        except Exception as e:  
            Logger.writeAndPrintLine("Could not connect specified database. "+self.connectionString1+": "+traceback.format_exc(), 3)    
            return False
        try:
            self.connection2=pyodbc.connect(self.connectionString2)
        except Exception as e:  
            Logger.writeAndPrintLine("Could not connect specified database. "+self.connectionString2+": "+traceback.format_exc(), 3)    
            return False
        return True

    def disconnect(self):
        self.connection1.close()
        self.connection2.close()

    def run(self):
        while True:
            self.runTableSyncs()
            time.sleep(60*self.syncInterval)
            break

    def runTableSyncs(self):
        if(not self.connect()):
            return
        for tablem in self.tableMaps:
            self.runSync(tablem)

        self.disconnect()

    def runSync(self, tablem):
        select1=self.buildSelectQuery(tablem.table1)
        select2=self.buildSelectQuery(tablem.table2)
        cursor1=self.connection1.cursor()
        cursor2=self.connection1.cursor()
        cursor1.execute(select1)
        cursor2.execute(select2)
        
        row1=cursor1.fetchone()
        row2=cursor2.fetchone()
        print(row1[1])
        print(row2)
        while True:

            if(not row1 and not row2):
                break
            if(row1[0]==row2[0] and row1[1]==row2[1]): #nothing to do here
                row1=cursor1.fetchone()
                row2=cursor2.fetchone()
            else:
                #rows are different, how we handle this is based on direction setting.  
                if(row1[0]>row2[0] and tablem.direction==1):
                    cursor2.fetchone() # catch up cursor 2. 
                if(row1[0]>row2[0] and tablem.direction==2):




    def buildSelectQuery(self, temptable):
        return "SELECT * FROM "+temptable.tableName+" WHERE CASENUM='243441' ORDER BY "+temptable.pkCol+" ASC"

class tablemap(object):
    #1 for 1->2, 2 for 1<->2
    name=""
    direction=0
    table1=None
    table2=None

class table(object):
    tableName=""
    pkCol=""
    modTimeCol="" #will be used to determine tiebreaker in direction #3
