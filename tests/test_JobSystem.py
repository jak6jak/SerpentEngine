import time
import datetime
from unittest import TestCase
from pygame_extended.JobSystem import WorkerThread, init, submit, thread_index
import multiprocessing

results_lock = multiprocessing.Lock()


def testFunction(results, i):
    global results_lock
    with results_lock:
        results.append(i)


def testFunctionSpawnAnother(results, i):
    with results_lock:
        results.append(i)
    submit(testFunction(results, i))


def testFunctionIndex(results):
    global results_lock
    with results_lock:
        results.append(thread_index())
    time.sleep(.05)


class TestWorkerThread(TestCase):
    def test_should_submit_and_run_jobs(self):
        job_system = init(10, 100)

        global results_lock
        results = []

        for i in range(1000):
            future = submit(testFunction(results, i))

        del job_system

        with results_lock:
            self.assertEqual(len(results), 1000)
            self.assertTrue(any(x in results for x in range(1000)))

    def test_should_submit_job_within_job(self):
        job_system = init(10, 100)

        global results_lock
        results = []
        for i in range(1000):
            future = submit(testFunctionSpawnAnother(results, i))
        del job_system

        with results_lock:
            self.assertEqual(len(results), 2000)
            self.assertTrue(any(x in results for x in range(2000)))

    def test_should_run_job_when_buffer_is_full(self):
        job_system = init(100, 1)

        global results_lock
        results = []

        for i in range(200):
            future = submit(testFunction(results, i))

        with results_lock:
            self.assertEqual(len(results), 100)
            self.assertTrue(any(x in results for x in range(100, 200)))

        del results
        del job_system

    def test_should_get_thread_index(self):
        TIMEOUT = 50000

        job_system = init(10, 5)
        results = []
        start = datetime.datetime.now()
        while True:
            future = submit(testFunctionIndex(results))

            now = datetime.datetime.now()
            duration = now - start
            if duration.microseconds > TIMEOUT:
                break

        del job_system

        with results_lock:
            print(results)
            self.assertEqual(len(results), 5)
            self.assertTrue(any(x in results for x in range(0, 5)))
