#!/usr/bin/env python

import signal
import logging
import logging.handlers
import os
import time
import datetime
import sys
import cPickle as pickle
import numpy as np
import plotly.plotly as plotly
import atexit
from signal import SIGTERM

from plotly.graph_objs import *
import re

import pywapi
import ConfigParser

import MySQLdb as mdb

#set working directory to where "autoPlot.py" is
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

config = ConfigParser.ConfigParser()
config.read(dname+"/config.txt")

LOG_LOGFILE = config.get('logging', 'logfile')
logLevelConfig = config.get('logging', 'loglevel')
if logLevelConfig == 'info':
    LOG_LOGLEVEL = logging.INFO
elif logLevelConfig == 'warn':
    LOG_LOGLEVEL = logging.WARNING
elif logLevelConfig ==  'debug':
    LOG_LOGLEVEL = logging.DEBUG

LOGROTATE = config.get('logging', 'logrotation')
LOGCOUNT = int(config.get('logging', 'logcount'))

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LOGLEVEL)
handler = logging.handlers.TimedRotatingFileHandler(LOG_LOGFILE, when=LOGROTATE, backupCount=LOGCOUNT)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class MyLogger(object):
        def __init__(self, logger, level):
                self.logger = logger
                self.level = level

        def write(self, message):
                # Only log if there is a message (not just a new line)
                if message.rstrip() != "":
                        self.logger.log(self.level, message.rstrip())

#sys.stdout = MyLogger(logger, logging.INFO)
#sys.stderr = MyLogger(logger, logging.ERROR)

CONN_PARAMS = (config.get('main','mysqlHost'), config.get('main','mysqlUser'),
               config.get('main','mysqlPass'), config.get('main','mysqlDatabase'),
               int(config.get('main','mysqlPort')))

MYSQL_BACKUP_DIR = config.get('main','mysqlBackupDir')

PLOTLY_USER = config.get('main','plotlyUser')
PLOTLY_KEY = config.get('main','plotlyKey')

PLOTLY_ID1 = os.path.split(config.get('main','plotlyPlot1'))[-1]
PLOTLY_ID2 = os.path.split(config.get('main','plotlyPlot2'))[-1]
PLOTLY_ID3 = os.path.split(config.get('main','plotlyPlot3'))[-1]
PLOTLY_ID4 = os.path.split(config.get('main','plotlyPlot4'))[-1]

plotly.sign_in(PLOTLY_USER, PLOTLY_KEY)


class autoPlotDaemon():
    def getThermSet(self):
        conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])

        cursor = conn.cursor()

        cursor.execute("SELECT * FROM ThermostatSet")
        thermSet = cursor.fetchall()

        cursor.close()
        conn.close()

        return thermSet[0][1:-1]

    def getProg(self):
        conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])

        cursor = conn.cursor()

        cursor.execute("SELECT * FROM ProgramTypes")
        prog = cursor.fetchall()
        actProg = 'Null'

        for pair in prog:
            if pair[1] == 1:
                actProg = pair[0]


        cursor.close()
        conn.close()

        return actProg

    def getProgTimes(self,progStr):
        conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])
        cursor = conn.cursor()

        dayDict = {'MON': 0, 'TUE': 1, 'WED': 2, 'THU': 3, 'FRI': 4, 'SAT': 5, 'SUN': 6}

        if progStr == 'Seven Day':
            cursor.execute("SELECT weekDay,time FROM ManualProgram")
            progTimes = cursor.fetchall()
        elif progStr == 'Smart':
            cursor.execute("SELECT weekDay,time FROM SmartProgram")
        else:
            cursor.close()
            return []


        progTimes = [list(pair) for pair in progTimes]
        progDT = []
        for pair in progTimes:
            pair[1] = (datetime.datetime.min + pair[1]).time()
            pair[0] = self.next_weekday(dayDict[pair[0]],pair[1])

            progDT.append(datetime.datetime.combine(pair[0],pair[1]))

        cursor.close()
        conn.close()

        return progDT

    def next_weekday(self, weekday, tod):
        d = datetime.datetime.now()
        days_ahead = weekday - d.date().weekday()

        if days_ahead < 0: # Target day already happened this week
            days_ahead += 7
        if days_ahead == 0:
            if d.time() > tod:
                days_ahead += 7

        return d + datetime.timedelta(days_ahead)

    def createPlots(self, now):
        plotLinks = []

	sensorUrls = self.sensorPlots(now)
        for url in sensorUrls:
            plotLinks.append(url)

	controlUrls = self.controlPlots(now)
        for url in controlUrls:
            plotLinks.append(url)

        conn = mdb.connect(CONN_PARAMS[0], CONN_PARAMS[1], CONN_PARAMS[2], CONN_PARAMS[3], port=CONN_PARAMS[4])

        cursor = conn.cursor()

        cursor.execute('SELECT timeStamp,coolOn,heatOn,fanOn,auxOn from ThermostatLog')

        statData = np.asarray(cursor.fetchall())

        statMonth = np.asarray([data for data in statData if data[0].month == now.month and
                                        data[0].year == now.year])
        monthSec = statMonth[:, 0]
        monthSec = np.asarray([int(time.strftime('%s')) for time in monthSec])

        monthCoolHours = np.trapz(statMonth[:,1],monthSec)/3600.0
        monthHeatHours = np.trapz(statMonth[:,2],monthSec)/3600.0
        monthAuxHours = np.trapz(statMonth[:,4],monthSec)/3600.0

        statDay = np.asarray([data for data in statData if (now-data[0]).days < 1])

        daySec = statDay[:, 0]
        daySec = np.asarray([int(time.strftime('%s')) for time in daySec])

        dayCoolHours = np.trapz(statDay[:,1],daySec)/3600.0
        dayHeatHours = np.trapz(statDay[:,2],daySec)/3600.0
        dayAuxHours = np.trapz(statDay[:,4],daySec)/3600.0

        conn.commit()
        cursor.close()
        conn.close()

        return (plotLinks, '%0.2f' % (monthHeatHours+monthCoolHours), '%0.2f' % (dayHeatHours+dayCoolHours),
                '%0.2f' % monthAuxHours, '%0.2f' % dayAuxHours, now)

    def sensorPlots(self, now):
        conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])

        cursor = conn.cursor()

        cursor.execute('SELECT moduleID from ModuleInfo')
        modIDs = [int(entry[0]) for entry in cursor.fetchall()]
       
        dayPlot = plotly.get_figure(PLOTLY_USER, file_id=PLOTLY_ID1)
        monthPlot = plotly.get_figure(PLOTLY_USER, file_id=PLOTLY_ID2)

        dataList=[]
        monthData=[]
        dayData=[]

        for colorInd, mod in enumerate(modIDs):
            cursor.execute('SELECT timestamp,temperature,location from SensorData WHERE moduleID=%s'%(str(mod)))
            dataList.append(np.asarray(cursor.fetchall()))
            
            colors = ['blue', 'orange', 'green', 'yellow', 'red']
            try:
                # Load every 20th temp reading 
                plotMonth = np.asarray([[data[0],float(data[1]),data[2]] for ind,data in enumerate(dataList[-1]) if ind%20 == 0])
                
                # Now, use only those elements for this month and year
                plotMonth = np.asarray([data for data in plotMonth if data[0].month == now.month and data[0].year == now.year])

                plotDay = np.asarray([ [ data[0],float(data[1]),data[2] ]for data in dataList[-1] if (now-data[0]).days < 1])

		if plotMonth.shape[0] != 0:
                  monthData.append(Scatter(x=plotMonth[:,0],y=plotMonth[:,1],mode='lines',
                                           name=plotMonth[0,2],line=Line(color=colors[colorInd],width=1)))

                if plotDay.shape[0] != 0:
                  dayData.append(Scatter(x=plotDay[:,0],y=plotDay[:,1],mode='lines',name=plotDay[0,2],
                                         line=Line(color=colors[colorInd],width=1)))
            except IndexError:
                pass
            
        dayPlot['data']=Data(dayData)
        if len(dayPlot.data) > 0:
          day_url = plotly.plot(dayPlot,filename='day_plot', auto_open=False)

        monthPlot['data']=Data(monthData)
        if len(monthPlot.data) > 0:
          month_url = plotly.plot(monthPlot,filename='month_plot', auto_open=False)

        conn.close()

        return {'url':day_url,'name':'24Hr All'}, {'url':month_url,'name':'Month All'}

    def controlPlots(self, now):
        conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])

        cursor = conn.cursor()

        dayPlot = plotly.get_figure(PLOTLY_USER, file_id=PLOTLY_ID3)
        monthPlot = plotly.get_figure(PLOTLY_USER, file_id=PLOTLY_ID4)

        cursor.execute('SELECT timestamp,targetTemp,actualTemp from ThermostatLog')
        controlData = np.asarray(cursor.fetchall())

        cursor.execute("SELECT timestamp,temperature from SensorData WHERE moduleID=0")
        weatherData = np.asarray(cursor.fetchall())

        monthData = []
        dayData = []
        try:

            plotMonth = np.asarray([[data[0],float(data[1]),float(data[2])] for ind,data in enumerate(controlData) if ind%20 == 0])
            plotMonth = np.asarray([data for data in plotMonth if data[0].month == now.month and
                                    data[0].year == now.year])

            weatherMonth = np.asarray([[data[0],float(data[1])] for ind,data in enumerate(weatherData) if ind%20 == 0])
            weatherMonth = np.asarray([data for data in weatherMonth if data[0].month == now.month and
                                    data[0].year == now.year])


            monthData.append(Scatter(x=plotMonth[:,0],y=plotMonth[:,1],mode='lines',
                                     name='Target Month',line=Line(color='#219ab3',width=2,dash='dot')))

            monthData.append(Scatter(x=plotMonth[:,0],y=plotMonth[:,2],mode='lines',
                                     name='Actual Month',line=Line(color='blue',width=1)))

            if weatherMonth.shape[0] > 0:
                monthData.append(Scatter(x=weatherMonth[:,0],y=weatherMonth[:,1],mode='lines',
                                         name='External',line=Line(color='orange',width=1)))


            plotDay = np.asarray([[data[0],float(data[1]),float(data[2])]for data in controlData if (now-data[0]).days < 1])


            weatherDay = np.asarray([[data[0],float(data[1])]for data in weatherData if (now-data[0]).days < 1])


            dayData.append(Scatter(x=plotDay[:,0],y=plotDay[:,1],mode='lines',name='Target Day',
                                   line=Line(color='#219ab3',width=2,dash='dot')))

            dayData.append(Scatter(x=plotDay[:,0],y=plotDay[:,2],mode='lines',name='Actual Day',
                                   line=Line(color='blue',width=1)))

            if weatherDay.shape[0] > 0:
                dayData.append(Scatter(x=weatherDay[:,0],y=weatherDay[:,1],mode='lines',name='External',
                                       line=Line(color='orange',width=1)))

        except IndexError:
            pass

        dayPlot['data']=Data(dayData)
        try:
            day_url = plotly.plot(dayPlot,filename='day_plot_control',auto_open=False)

            monthPlot['data']=Data(monthData)
            month_url = plotly.plot(monthPlot,filename='month_plot_control',auto_open=False)
        except PlotlyError, err:
            logger.error('Plotting Exception')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error('Error occurred at %s'%(datetime.datetime.now().strftime('%m-%d-%y-%X')))
            logger.error(str(exc_type.__name__))
            logger.error(err)
            logger.error(str(fname))
            logger.error(str(exc_tb.tb_lineno))
            
        cursor.close()
        conn.close()

        return {'url':day_url,'name':'24Hr Control'}, {'url':month_url,'name':'Month Control'}

    def backupDB(self):
        conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])

        cursor = conn.cursor()
        timestamp = datetime.datetime.now().strftime('%y-%m-%d-%X')
        timestamp = re.sub(':', '-', timestamp)
        backDir = MYSQL_BACKUP_DIR

        cursor.execute("SELECT * INTO OUTFILE '%s' FROM ThermostatLog"%(os.path.join(backDir,'ThermostatLog-'+timestamp+'.csv')))
        conn.commit()

        cursor.execute("SELECT * INTO OUTFILE '%s' FROM SensorData"%(os.path.join(backDir,'SensorData-'+timestamp+'.csv')))
        conn.commit()

        cursor.execute("DELETE FROM SensorData WHERE timeStamp < TIMESTAMP(DATE_SUB(NOW(), INTERVAL 35 DAY))")
        conn.commit()

        cursor.execute("DELETE FROM ThermostatLog WHERE timeStamp < TIMESTAMP(DATE_SUB(NOW(), INTERVAL 35 DAY))")
        conn.commit()

        cursor.close()
        conn.close()

        fobj = open('lastBackup.pck','wb')
        pickle.dump(datetime.datetime.now(),fobj)
        fobj.close()

    def run(self,debug=False):
        plot = False
        backup = False
	first = True
        while True:
            try:
                curModule, targTemp, targMode, expTime = self.getThermSet()
                weekList = ['MON','TUE','WED','THU','FRI','SAT','SUN']
                curTime = datetime.datetime.now()

                actProg = self.getProg()
                timeList = self.getProgTimes(actProg)

                if curTime>expTime or first:

                    first = False

                    conn = mdb.connect(CONN_PARAMS[0],CONN_PARAMS[1],CONN_PARAMS[2],CONN_PARAMS[3],port=CONN_PARAMS[4])

                    cursor = conn.cursor()

                    if actProg=='Manual':
                        cursor.execute("UPDATE ThermostatSet SET expiryTime='%s' WHERE entryNo=1"
                                       %(str(datetime.datetime.now()+datetime.timedelta(days=1))))
                    else:
                        diffList = [datetime.datetime.now()-timeObj for timeObj in timeList]
                        sortedInds = sorted(range(len(diffList)), key=lambda k: diffList[k])

                        keepInd = [ind for ind in sortedInds if diffList[ind].total_seconds()<0]
                        rowKey = keepInd[0]

                        newExp = timeList[sortedInds[-1]]

                        if actProg == 'Seven Day':
                            cursor.execute("SELECT * FROM ManualProgram WHERE rowKey=%s" % (str(rowKey+1)))
                        elif actProg == 'Smart':
                            cursor.execute("SELECT * FROM SmartProgram WHERE rowKey=%s" % (str(rowKey+1)))

                        newData = cursor.fetchall()[0]

                        cursor.execute("UPDATE ThermostatSet SET moduleID=%s, targetTemp=%s, targetMode='%s', expiryTime='%s' WHERE entryNo=1"
                               %(str(newData[3]),str(newData[4]),str(newData[5]),str(newExp)))


                    conn.commit()
                    cursor.close()
                    conn.close()

                #########################################
                ##### Check about plotting
                #########################################
                try:
                    fobj = open('plotData.pck','rb')
                    lastPlot = pickle.load(fobj)[-1]
                    fobj.close()

                    if (curTime-lastPlot).total_seconds()>300:
                        plot = True
                except:
                    plot = True

                if plot:
                    plotData = self.createPlots(curTime)

                    fobj = open("plotData.pck", "wb")
                    pickle.dump(plotData,fobj)
                    fobj.close()


                #########################################
                ##### Check about backups
                #########################################
                try:
                    fobj = open('lastBackup.pck','rb')
                    lastBackup = pickle.load(format())
                    fobj.close()

                    if (curTime-lastBackup).days>30:
                        backup = True
                except:
                    backup = True

                if backup:
                    self.backupDB()

                time.sleep(360)

            except Exception:#IOError:#
                if debug:
                    raise
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

                logger.error('Error occurred at %s'%(datetime.datetime.now().strftime('%m-%d-%y-%X')))
                logger.error(str(exc_type.__name__))
                logger.error(str(fname))
                logger.error(str(exc_tb.tb_lineno))

                time.sleep(5)


def sigterm_handler(_signo, _stack_frame):
    "When sysvinit sends the TERM signal, cleanup before exiting."
    logger.info("Received signal {}, exiting...".format(_signo))
    print("Received signal {}, exiting...".format(_signo))
    logger.info("Stopping Daemon due to signal")
    sys.exit(0)

signal.signal(signal.SIGTERM, sigterm_handler)

logger.debug("Starting Daemon")

autoplot = autoPlotDaemon()
autoplot.run()
logger.debug("Stopping Daemon")

