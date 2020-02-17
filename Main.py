import threading, time, queue, database, datacollector, multiprocessing, traceback
import RPi.GPIO as gpio
import matplotlib.pyplot as plt

from tkinter import *
from logging import *

# Main thread (foreground): contains the GUI, user controls.


# Global variables
#
# Data collector/User Thread -> Database Queue
dbQueue = multiprocessing.Queue()       # Data Collector/User -> Database. Unbounded, thread-safe queue              
returnQueue = multiprocessing.Queue()   # Database -> User Thread Queue

# The max granularity of the collected data in seconds
MAX_GRANULARITY = 5

# water pump pin
pump = 17
# Time (in seconds) to pump water
pumpTime = 3

# GPIO setup
gpio.setmode(gpio.BCM)
gpio.setup(pump,gpio.OUT)
gpio.output(pump,False)

# Allows the user to enable/disable database and datacollector
dbEnabled = True # Database enabled
dcEnabled = True # Datacollector enabled

# Activates the pump for 'pumpTime' seconds
def pumpWater():
    gpio.output(pump, True)
    time.sleep(pumpTime)
    gpio.output(pump,False)


# Calls graph() in another thread, as to not disrupt the user experience (GUI unresponsiveness)
def createGraph(mins):
    graphThread = threading.Thread(target=graph,args=(mins))
    graphThread.start()

    
# Creates a line-plot from now to (now - n mins) displaying soil moisture
def graph(mins):
    while(returnQueue.qsize() > 0): # Empties the returnQueue of existing data.
        returnQueue.get()
    end = int(time.time())
    start = end-(mins*60)  # Starting timestamp
    task = database.OutputTask([start,end]) # Output task -> Database
    dbQueue.put(task)
    # Waits until the data is returned or until 15 seconds has passed.
    while(returnQueue.qsize() < 1 and (time.time() < end+15)):
        time.sleep(0.2)
    data = returnQueue.get()
    # Formats data for us in the graph:
    #
    ts = [] # timestamps
    sensors = [[],[]]
    for row in data:
        ts.append(row[0])
        for i in range(1,len(row)):
            sensors[i-1].append(row[i])
    tBase = ts[0]
    for i in range(len(ts)):
        ts[i] = ts[i]-tBase
    #
    # x and y ranges for the graph
    plt.xlim(0,ts[-1])
    plt.ylim(0,100)

    # Plots both sensors to the graph
    for sensor in sensors:
        plt.plot(ts,sensor)
    # The legend for the graph
    #plt.legend("Top Soil","Bottom Soil")
    # Displays the graph.
    plt.show()

if(__name__=="__main__"):
    # Start database thread
    dbThread = threading.Thread(target=database.databaseConnector,args=(dbQueue,returnQueue,dbEnabled))
    dbThread.start()
    # Start data collector thread
    collectorThread = threading.Thread(target=datacollector.dataCollector,args=(dbQueue,MAX_GRANULARITY,dcEnabled))
    collectorThread.start()

    # GUI, user control here
    gui = Tk()
    frame = Frame(gui)
    frame.pack()

    # GUI widgets here:
    #
    # Pump Button
    pumpButton = Button(frame, text="Pump", command=pumpWater)
    pumpButton.pack()

    # Graphing buttons with labels
    fiveButton = Button(frame, text="5 min.", command= lambda :createGraph(5))
    fiveButton.pack()
    sixtyButton = Button(frame, text="60 min.", command= lambda :createGraph(60))
    sixtyButton.pack()
    twelveHourButton = Button(frame, text="12 hr.", command= lambda :createGraph(12*60))
    twelveHourButton.pack()

    gui.mainloop()
