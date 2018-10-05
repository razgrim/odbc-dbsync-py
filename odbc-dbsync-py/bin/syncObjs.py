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
    connection1w=None
    connection2w=None

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
            self.connection1w=pyodbc.connect(self.connectionString1)
        except Exception as e:  
            Logger.writeAndPrintLine("Could not connect specified database. "+self.connectionString1+": "+traceback.format_exc(), 3)    
            return False
        try:
            self.connection2=pyodbc.connect(self.connectionString2)
            self.connection2w=pyodbc.connect(self.connectionString2)
        except Exception as e:  
            Logger.writeAndPrintLine("Could not connect specified database. "+self.connectionString2+": "+traceback.format_exc(), 3)    
            return False
        return True

    def disconnect(self):
        self.connection1.close()
        self.connection2.close()
        self.connection1w.close()
        self.connection2w.close()

    def run(self):
        while True:
            self.runTableSyncs()
            time.sleep(60*self.syncInterval)

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
        print(row1.casenum)
        print(row1)
        print(row2)
        while True:
            if(not row1 and not row2):#reached the end, quit. 
                break
            if(row1[0]==row2[0] and row1[1]==row2[1]): #nothing to do here
                row1=cursor1.fetchone()
                row2=cursor2.fetchone()

            #insert checking
            elif(row1[0]>row2[0]):
                if(tablem.direction==1):
                    cursor2.fetchone() #1 way sync we don't care that table 1 is missing a row
                else:
                    None #TODO insert row2 in table 1
                    Logger.writeAndPrintLine("Inserting row "+row2[0]+" from table 2 to 1.", 1) 
                    cursor2.fetchone()
            elif(row1[0]<row2[0]): #row2 skipped over 1. table2 is missing row1. 
                None #TODO insert row1 in table2, for both 1 and 2 way sync.
                Logger.writeAndPrintLine("Inserting row "+row1[0]+" from table 1 to 2.", 1) 
                cursor1.fetchone()

            #update checking 
            else:
                #rows not equal, but indexes are. the modifieds must be different. 
                if(row1[1]>row2[1]):
                    None #TODO update row2 with row1
                elif(tablem.direction==2): #row1 is older than row2 and 2 way sync
                    None #TODO update row1 with row2
                row1=cursor1.fetchone()
                row2=cursor2.fetchone()


    def buildSelectQuery(self, temptable):
        return "SELECT "+temptable.pkCol+","+temptable.modTimeCol+",* FROM "+temptable.tableName+" WHERE CASENUM='243441' ORDER BY "+temptable.pkCol+" ASC"

    def doInsert(self, sourcerow, targettable, writeconnection):
        None

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
