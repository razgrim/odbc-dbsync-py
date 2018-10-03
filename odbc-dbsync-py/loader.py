import os
import configparser
from Logger import Logger

class Loader(object):

    #config obj
    config = None
    configFileName="dbsync.config"

    #global params
    runOnce = False
    idle = 60
    logDir = "log"

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

    def launch(self):
        print("Program started.")
        self.loadConfig()
        Logger.logFile=self.logDir+'\\application.log'
        Logger.writeAndPrintLine("Configuration loaded successfully.",0)
        print("Launching VoicemailTranscriber with the following parameters! :")
        self.printConfig()

    def loadSyncs(self):
        files = os.listdir("sync")


