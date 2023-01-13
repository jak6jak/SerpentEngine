# https://www.rismosch.com/article?id=building-a-job-system
import copy
import multiprocessing


class BlockedOrFull(Exception):
    """Raised when jobBuffer is full or the lock is locked"""

    def __init__(self, not_pushed, message="JobBuffer is full or locked"):
        self.not_pushed = not_pushed
        self.message = message
        super().__init__(self.message)


class IsEmpty(Exception):
    """Raised if Job Buffer is empty"""
    pass


class BlockedOrEmpty(Exception):
    """Raised if the jobbuffer is empty or blocked"""


class JobBuffer:
    def __init__(self, capacity):
        self.capacity = capacity
        self.head = 0
        self.tailLock = multiprocessing.Lock()
        self.tail = 0
        self.jobsLock = multiprocessing.Lock()
        self.jobs = [None] * capacity

    def push(self, job):
        node = None
        successfully_acquired = self.jobsLock.acquire(False)
        if successfully_acquired:
            try:
                node = self.jobs[self.head]
                if node is not None:
                    return BlockedOrFull(job)
                else:
                    self.jobs[self.head] = job
                    self.head = (self.head + 1) % self.capacity
            finally:
                self.jobsLock.release()
        else:
            return Exception("Mutex is poisoned")

    def wait_and_pop(self):
        new_head = self.capacity - 1 if self.head == 0 else self.head - 1
        successfully_acquired = self.jobsLock.acquire(True)
        if successfully_acquired:

            try:
                node = self.jobs[new_head]
                if node is None:
                    return IsEmpty()
                else:
                    temp = self.jobs[new_head]
                    self.jobs[new_head] = None
                    self.head = new_head
                    return temp
            finally:
                self.jobsLock.release()
        else:
            raise Exception("Not acquired")

    def steal(self):
        successfully_acquired_tail = self.tailLock.acquire(False)
        if successfully_acquired_tail:
            try:
                old_tail = self.tail
                successfully_acquired_jobs = self.jobsLock.acquire(False)
                if successfully_acquired_jobs:
                    try:
                        node = self.jobs[old_tail]
                        if node is None:
                            return BlockedOrEmpty()
                        else:
                            self.jobs[old_tail] = None
                            self.tail = (old_tail + 1) % self.capacity
                            return node
                    finally:
                        self.jobsLock.release()
                else:
                    return BlockedOrEmpty()
            finally:
                self.tailLock.release()
        else:
            return BlockedOrEmpty()
