import threading, time, queue, database, os, sys, serial, multiprocessing
from logging import *


# Data collector file: contains datacollector function; 


# Data collector layer: collects data from sensors by way of the serial
#   connection with the Arduino (the Arduino is used the gather analog
#   data, which the RaspberryPi is unable to do itself without the aid
#   of an external converter).
def dataCollector(dbQueue,MAX_GRANULARITY,dcEnabled):
    
    # serial connection to arduino via USB
    arduino = serial.Serial("/dev/ttyUSB0",9600,timeout=1)
    
    # Global dbQueue         # Send input tasks to the database layer
    # Global max_granularity # Minimum time between reads

    # Read sensor data from the connected arduino
    # Reads a string: "top_sensor,bottom_sensor"
    def read_serial():
        try:
            data = arduino.readline()   # Reads a line from the Arduino
            data = data.strip()         # Removes white-space
            data = data.decode('utf-8') # Decodes the text (to utf-8)
            data = data.split(',')      # Splits the string into a list of string, delimitted by ',''s
            data[0] = int(data[0])      # Casts the string to an int
            data[1] = int(data[1])      # ||||||||||||||||||||||||||
            data.insert(0,int(time.time())) # Inserts a timestamp into index 0 of the list
            return(data) 
        except:
            print("Serial read error")
        
        
            
    while(True): # The dataCollector thread is always running
        while(dcEnabled): # Only collects and sends data when enabled
            time.sleep(float(MAX_GRANULARITY))
            data = read_serial()
            if(data != None):   # data is correctly formatted
                insert = database.InputTask(data) # Creates an input task of 
                dbQueue.put(insert) # Inserts the task into the database
            else:
                print("Improper data format")


# Uncomment for TESTING:
# q = multiprocessing.Queue()
# collectorThread = threading.Thread(target=dataCollector,args=(q,3,True))
# collectorThread.start()
# while(True):
#    print(q)
#    time.sleep(3)

