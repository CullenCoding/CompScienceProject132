import threading, time, queue, sqlite3, abc, multiprocessing, traceback
from logging import *

# Database file: contains database function and task objects

class QueuedTask(metaclass=abc.ABCMeta):
    def __init__(self,data):
        # input data to be in form of tuple/list [long/int timestamp, int s1, ... int sn]
        #   where sn is the data collected from sensor n at the listed time
        # output task (data) to be in form of tuple/list [long/int a, long/int b]
        pass
    def execute(self,db,rq):
        pass
        

class InputTask(QueuedTask): # Execute an insertion into the database of sensor data.
    def __init__(self,data):
        self.data = data
    def execute(self,db,rq):
        if(self.data == None):
            print("Improper data format")
        else:
            try:
                db.execute("INSERT INTO sensors VALUES ({},{},{})".format(self.data[0],self.data[1],self.data[2]))
            except Exception:
                print(traceback.format_exc())
                print("Database insert error: ",self.data)

class OutputTask(QueuedTask): # Executes a selection query from the database and send the data to the return queue.
    def __init__(self,data):
        self.data = data
    def execute(self,db,rq):
        print("\t",self.data)
        if(self.data == None):
            print("Improper data format")
        else:
            # Get the data from timestamp task.data[0] to timestamp task.data[1]
            # result_set query = SELECT * FROM sensorData WHERE timestamp >= task.data[0] AND timestamp <= task.data[1]
            try:
                resultSet = []
                for row in db.execute("SELECT * FROM sensors WHERE timestamp >= {} AND timestamp <= {}".format(self.data[0],self.data[1])):
                    resultSet.append(row)
                rq.put(resultSet)
            except Exception:
                print(traceback.format_exc())
                print("Database query error")

schema = """CREATE TABLE sensors (timestamp INTEGER, soil_1 INTEGER, soil_2 INTEGER)"""

# Database layer (The database is made thread-safe by using this separate thread
#   to insert or get data from the database. Queues are used to queue insert and
#   query tasks to the database layer.)
# Table: (to be expanded)
#  |------------------------------------------------------------|
#  | INTEGER timestamp | INTEGER soil_bottom | INTEGER soil_top |
#  |-------------------|---------------------|------------------|
#  | LONG integer      |       integer       |      integer     |
#
def databaseConnector(dbQueue,returnQueue,dbEnabled):
    
    # Connect to database
    dbCon = sqlite3.connect("sensordata.db")
    db = dbCon.cursor()
    
    # dbQueue             # Tasks from datacollector/user
    # returnQueue         # Used to return data to the user
    # dcEnabled           # Boolean, controls when database operates.

    while(True): # Database thread is always running
        while(dbEnabled): # Database will only insert/query when enabled
            if(dbQueue.qsize() > 0):
                # Will wait until data is available in the queue
                # The object from the Queue will be eiter an OutputTask or InputTask object, both inherriting from QueuedTask.
                task = dbQueue.get(timeout=None)
                task.execute(db,returnQueue)
                # Commits any changes to the database.
                dbCon.commit()

