import threading
import random


class LamportClock:
    def __init__(self, time, processID, queue):
        self.lock = threading.Lock()
        self.time = 1
        self.processID = processID

    def compareTime(self, other):
        self.lock.acquire()
        try:
            self.time = max(int(self.time), int(other)) + 1
            return self.time
        finally:
            self.lock.release()

    def incrementTime(self):
        self.lock.acquire()
        try:
            self.time = self.time + 1
        finally:
            self.lock.release()

    def getLocalTime(self):
        return self.time

    def getLamportTime(self):
        return (str(self.time) + "." + str(self.processID))
