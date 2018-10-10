import os
import io
import configparser
from Logger import Logger
from syncObjs import syncjob
from syncObjs import tablemap
from syncObjs import table
import traceback
from threading import Thread

class Loader(object):

    #config obj
    config = None
    configFileName="bin\dbsync.conf"

    #global params
    runOnce = False
    idle = 60
    logDir = "log"

    syncjobs=[]
    syncjobThreads=[]

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
        print("")

    def launch(self):
        print("Program started.")
        print(os.getcwd())
        self.loadConfig()
        Logger.logFile=self.logDir+'\\application.log'
        Logger.writeAndPrintLine("Configuration loaded successfully.",0)
        print("Launching VoicemailTranscriber with the following parameters! :")
        self.printConfig()
        self.loadSyncjobs()
        self.launchSyncjobs()

    def loadSyncjobs(self):
        files = os.listdir("sync")
        for file in files:
            if(not file.endswith(".job")):
                continue
            try:
                fileparser=configparser.ConfigParser()
                print("loading "+file)
                fileparser.read("sync\\"+file)
                tempSyncjob=Loader.parseSyncjob(fileparser)
                if(tempSyncjob.testConnection()):
                    self.syncjobs.append(tempSyncjob)
                else:
                    Logger.writeAndPrintLine("Failed to connect one or more databases for "+file+", skipping.",3)   
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
            tempTableMap.direction=int(fileparser[tableMapName]["direction"])
            tempTableMap.syncInterval=int(fileparser[tableMapName]["syncInterval"])

            tempTableMap.table1=table()
            tempTableMap.table1.tableName=fileparser[tableMapName]["table1Name"]
            tempTableMap.table1.pkCol=str(fileparser[tableMapName]["table1pkCol"]).upper().split(',')
            tempTableMap.table1.modTimeCol=fileparser[tableMapName]["table1modTimeCol"]
            tempTableMap.table1.dontUpdate=str(fileparser[tableMapName]["table1dontUpdate"]).upper().split(',')
            tempTableMap.table2=table()
            tempTableMap.table2.tableName=fileparser[tableMapName]["table2Name"]
            tempTableMap.table2.pkCol=str(fileparser[tableMapName]["table2pkCol"]).upper().split(',')
            tempTableMap.table2.modTimeCol=fileparser[tableMapName]["table2modTimeCol"]
            tempTableMap.table2.dontUpdate=str(fileparser[tableMapName]["table2dontUpdate"]).upper().split(',')
            tempjob.tableMaps.append(tempTableMap)

        return tempjob

    def launchSyncjobs(self):
        for job in self.syncjobs:
            tempThread=Thread(target = job.run())
            self.syncjobThreads.append(tempThread)
            tempThread.start()

