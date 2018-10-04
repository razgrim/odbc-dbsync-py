import os
import io
import configparser
from Logger import Logger
from syncObjs import syncjob
from syncObjs import tablemap
from syncObjs import table
import traceback

class Loader(object):

    #config obj
    config = None
    configFileName="bin\dbsync.conf"

    #global params
    runOnce = False
    idle = 60
    logDir = "log"

    syncjobs=[]

    def __init__(self):
        self.config = configparser.ConfigParser()

    def loadConfig(self):
        self.config.read(self.configFileName)

        #global params
        self.runOnce = self.config['DEFAULT']['runOnce']
        self.idle = int(self.config['DEFAULT']['idle'])
        self.logfile = self.config['DEFAULT']['runOnce']

    def printConfig(self):
        print("DEFAULT: ")
        print("runOnce: "+str(self.runOnce))
        print("idle: "+str(self.idle))
        print("logDir: "+str(self.idle))

    def launch(self):
        print("Program started.")
        print(os.getcwd())
        self.loadConfig()
        Logger.logFile=self.logDir+'\\application.log'
        Logger.writeAndPrintLine("Configuration loaded successfully.",0)
        print("Launching VoicemailTranscriber with the following parameters! :")
        self.printConfig()
        self.loadSyncjobs()

    def loadSyncjobs(self):
        files = os.listdir("sync")
        for file in files:
            if(not file.endswith(".job")):
                continue
            try:
                fileparser=configparser.ConfigParser()
                print("loading "+file)
                fileparser.read("sync\\"+file)
                self.syncjobs.append(Loader.parseSyncjob(fileparser))
            except Exception as e: 
                Logger.writeAndPrintLine("Error parsing configuration for "+file+": "+traceback.format_exc(),3)   
                continue

    def parseSyncjob(fileparser):
        tempjob=syncjob()
        tempjob.connectionString1=fileparser["SYNC"]["connectionString1"]
        tempjob.connectionString2=fileparser["SYNC"]["connectionString2"]

        for tableMapName in fileparser.sections():
            if(tableMapName=="SYNC"):
                continue
            tempTableMap=tablemap()
            tempTableMap.name=tableMapName
            tempTableMap.direction=fileparser[tableMapName]["direction"]

            tempTableMap.table1=table()
            tempTableMap.table1.tableName=fileparser[tableMapName]["table1Name"]
            tempTableMap.table1.pkCol=fileparser[tableMapName]["table1pkCol"]
            tempTableMap.table1.modTimeCol=fileparser[tableMapName]["table1modTimeCol"]
            tempTableMap.table2=table()
            tempTableMap.table2.tableName=fileparser[tableMapName]["table2Name"]
            tempTableMap.table2.pkCol=fileparser[tableMapName]["table2pkCol"]
            tempTableMap.table2.modTimeCol=fileparser[tableMapName]["table2modTimeCol"]
            tempjob.tableMaps.append(tempTableMap)

        return tempjob
