import pyodbc
import traceback
import time
from datetime import datetime
from Logger import Logger
from dateutil import parser

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

    filename=""

    def __init__(self, filename):
        self.filename=filename
        self.tableMaps=[]#apparently without this line, tableMaps acts statically
    
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
        Logger.writeAndPrintLine("job "+self.filename+" started sync "+tablem.name+", table "+tablem.table1.tableName+" and "+tablem.table2.tableName,0)
        select1=self.buildSelectQuery(tablem.table1)
        select2=self.buildSelectQuery(tablem.table2)
        cursor1=self.connection1.cursor()
        cursor2=self.connection2.cursor()
        cursor1.execute(select1)
        cursor2.execute(select2)
        row1=cursor1.fetchone()
        row2=cursor2.fetchone()

        columns = [column[0] for column in cursor1.description]
        columns = columns[len(tablem.table1.pkCol)+1:]

        while True:
            if(not row1 and not row2):#reached the end, quit. 
                break
            if(row1 and row2 and row1[0:len(tablem.table1.pkCol)+1]==row2[0:len(tablem.table1.pkCol)+1]): #nothing to do here
                row1=cursor1.fetchone()
                row2=cursor2.fetchone()
            ###insert checking###
            #check for nulls
            elif(not row1):
                if(tablem.direction==1):
                    break #if row1 is null we've reached the end of that table. we don't care about anything else in 1 way sync. 
                else:
                    self.doInsert(row2, tablem.table1,1,columns,self.connection1w) 
                    row2=cursor2.fetchone()
            elif(not row2): 
                self.doInsert(row1, tablem.table2,2,columns,self.connection2w) 
                row1=cursor1.fetchone()        
            else:
                #check for missed indices
                indexComparison=self.doPKsMatch(row1,row2,tablem.table1)
                if(indexComparison>0):
                    if(tablem.direction==1):
                        row2=cursor2.fetchone() #1 way sync we don't care that table 1 is missing a row
                    else:
                        self.doInsert(row2, tablem.table1,1,columns,self.connection1w)
                        row2=cursor2.fetchone()
                elif(indexComparison<0): 
                    self.doInsert(row1, tablem.table2,2,columns,self.connection2w)
                    row1=cursor1.fetchone()

                ###update checking###
                else:
                    #rows not equal, but indexes are. the modifieds must be different. 
                    if(not row2[len(tablem.table2.pkCol)]):
                        self.doUpdate(row1, tablem.table2,2,columns,self.connection2w)
                    elif((not row1[len(tablem.table1.pkCol)]) and direction==2):
                        self.doUpdate(row2, tablem.table1,1,columns,self.connection1w)
                    elif(parser.parse(str(row1[len(tablem.table1.pkCol)]))>parser.parse(str(row2[len(tablem.table2.pkCol)]))):
                        #print(str(parser.parse(str(row1[len(tablem.table1.pkCol)])))+'|'+str(parser.parse(str(row2[len(tablem.table2.pkCol)]))))
                        self.doUpdate(row1, tablem.table2,2,columns,self.connection2w)
                    elif(tablem.direction==2): #row1 is older than row2 and 2 way sync
                        self.doUpdate(row2, tablem.table1,1,columns,self.connection1w)
                    row1=cursor1.fetchone()
                    row2=cursor2.fetchone()
        self.connection1w.commit()
        self.connection2w.commit()
        Logger.writeAndPrintLine("sync "+tablem.name+" complete.",0)


    def buildSelectQuery(self, temptable):
        query="SELECT "
        for pk in temptable.pkCol:
            query=query+temptable.tableName+'.'+pk+', '
        query=query.rstrip(",")+temptable.modTimeCol+",* FROM "+temptable.tableName+" ORDER BY "
        for pk in temptable.pkCol:
            query=query+temptable.tableName+'.'+pk+' ASC,'
        query=query.rstrip(",")
        return query

    def doPKsMatch(self,row1,row2,temptable):
        colNum=0
        while colNum<len(temptable.pkCol):
            if(row1[colNum]>row2[colNum]):
                return 1
            elif(row1[colNum]<row2[colNum]):
                return -1
            colNum+=1
        return 0

    def doInsert(self, sourcerow, targettable, dbnum, columns, writeconnection):
        id=str(sourcerow[:(len(targettable.pkCol))])
        sourcerow=sourcerow[(len(targettable.pkCol)+1):]
        #insert statement
        query="INSERT INTO "+targettable.tableName+"("
        for columnName in columns:
            query=query+'"'+columnName+'",'

        #value statement
        query=query.rstrip(',')+") VALUES ("
        for rowval in sourcerow:
            if(rowval==None):
                query=query+"null,"
            else:
                query=query+"'"+str(rowval).replace("'","''")+"',"
        query=query.rstrip(',')+')'
        #print(query)
        #execute
        tempcursor=writeconnection.cursor()
        try:
            tempcursor.execute(query)
            Logger.writeAndPrintLine("Inserted row "+str(id)+" into db"+str(dbnum)+", "+targettable.tableName,0 )
        except Exception as e:  
            Logger.writeAndPrintLine("Could not insert row. "+str(id)+" into db"+str(dbnum)+", "+traceback.format_exc(), 3)  
        #input()

    def doUpdate(self, sourcerow, targettable, dbnum, columns, writeconnection):

        whereStatement=" WHERE "
        colNum=0
        id=str(sourcerow[:(len(targettable.pkCol))])
        for pk in targettable.pkCol:
            whereStatement=whereStatement+'"'+pk+'"'+"='"+str(sourcerow[colNum])+"' AND "
            colNum+=1
        whereStatement=whereStatement.rstrip(" AND ")
        sourcerow=sourcerow[(len(targettable.pkCol)+1):]
        query="UPDATE "+targettable.tableName+" SET "
        i=0
        for column in columns:
            if(column.upper() not in targettable.dontUpdate):
                if(sourcerow[i]==None):
                    query=query+'"'+column+'"=null,'
                else:
                    query=query+'"'+column+'"='+"'"+str(sourcerow[i]).replace("'","''")+"',"
            i+=1
        query=query.rstrip(",")
        query=query+whereStatement
        #print(query)
        tempcursor=writeconnection.cursor()
        try:
            tempcursor.execute(query)
            Logger.writeAndPrintLine("Updated row "+id+" into db"+str(dbnum)+", "+targettable.tableName,0 )
        except Exception as e:  
            Logger.writeAndPrintLine("Could not update row. "+id+" into db"+str(dbnum)+", "+traceback.format_exc(), 3)  

class tablemap(object):
    #1 for 1->2, 2 for 1<->2
    name=""
    direction=0
    table1=None
    table2=None

class table(object):
    tableName=""
    pkCol=[]
    modTimeCol="" #will be used to determine updates.
    modColNum=1
    dontUpdate=[] #columns NOT to update. Should contain primary keys (non-updatable things).
