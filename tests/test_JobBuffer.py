import threading
from copy import copy, deepcopy
from unittest import TestCase
import functools
from pygame_extended.JobBuffer import JobBuffer
from pygame_extended.JobBuffer import BlockedOrFull, IsEmpty, BlockedOrEmpty


def testJob(data):
    return data


def testJob2(data):
    return data * 2


def testJob3(data):
    return data * 3


class TestJobBuffer(TestCase):
    def test_push_and_pop(self):
        job_buffer = JobBuffer(4)

        job_buffer.push(functools.partial(testJob, 42))
        job = job_buffer.wait_and_pop()
        assert job() == 42, "Returned wrong data"

    def test_should_push_and_steal(self):
        job_buffer = JobBuffer(4)

        job_buffer.push(functools.partial(testJob, 42))

        job = job_buffer.steal()
        assert job() == 42, "Returned wrong data"

    def test_should_push_till_full(self):
        job_buffer = JobBuffer(2)

        data = 2
        job_buffer.push(functools.partial(testJob, 1))
        job_buffer.push(functools.partial(testJob2, 2))
        e = job_buffer.push(functools.partial(testJob3, 3))
        self.assertTrue(isinstance(e, BlockedOrFull))
        self.assertEqual(e.not_pushed(), 9)

    def test_should_pop_till_empty(self):
        job_buffer = JobBuffer(4)

        job_buffer.push(functools.partial(testJob, 1))
        job_buffer.push(functools.partial(testJob2, 2))

        pop1 = job_buffer.wait_and_pop()
        pop2 = job_buffer.wait_and_pop()

        pop3 = job_buffer.wait_and_pop()
        self.assertEqual(pop1(), 4)
        self.assertEqual(pop2(), 1)
        self.assertTrue(isinstance(pop3, IsEmpty), "Not empty")

    def test_should_steal_till_empty(self):
        job_buffer = JobBuffer(4)

        job_buffer.push(functools.partial(testJob, 1))
        job_buffer.push(functools.partial(testJob2, 2))

        steal1 = job_buffer.steal()
        steal2 = job_buffer.steal()
        steal3 = job_buffer.steal()
        self.assertTrue(isinstance(steal3, BlockedOrEmpty))
        self.assertEqual(steal1(), 1)
        self.assertEqual(steal2(), 4)

    def test_should_push_pop_and_steal_multiple_times(self):
        job_buffer = JobBuffer(5)

        for i in range(5):
            job_buffer.push(functools.partial(testJob, 1))
            job_buffer.push(functools.partial(testJob, 2))
            job_buffer.push(functools.partial(testJob, 3))
            job_buffer.push(functools.partial(testJob, 4))
            job_buffer.push(functools.partial(testJob, 5))
            e = job_buffer.push(functools.partial(testJob, 6))
            self.assertTrue(isinstance(e, BlockedOrFull))
            self.assertEqual(e.not_pushed(), 6)
            steal1 = job_buffer.steal()
            pop2 = job_buffer.wait_and_pop()
            steal3 = job_buffer.steal()
            pop4 = job_buffer.wait_and_pop()
            steal5 = job_buffer.steal()

            # error cases
            pop6 = job_buffer.wait_and_pop()

            steal7 = job_buffer.steal()
            self.assertEqual(steal1(), 1)
            self.assertEqual(pop2(), 5)
            self.assertEqual(steal3(), 2)
            self.assertEqual(pop4(), 4)
            self.assertEqual(steal5(), 3)
            self.assertTrue(isinstance(pop6, IsEmpty), "Not empty")
            self.assertTrue(isinstance(steal7, BlockedOrEmpty), "Not empty")

    """
    def test_should_push_to_orginal_and_pop_from_duplicate(self):
        orignal_buffer = JobBuffer(4)
        duplicated_buffer = deepcopy(orignal_buffer)

        job1 = functools.partial(testJob, 1)
        job2 = functools.partial(testJob, 2)

        push1 = orignal_buffer.push(job1)
        push2 = orignal_buffer.push(job2)

        pop1 = duplicated_buffer.wait_and_pop()
        steal2 = duplicated_buffer.steal()
    """

    # Multithreaded tests

    def test_should_steal_from_empty_buffer_from_multiple_threads(self):
        buffer = JobBuffer(1000)
        handles = []
        results_lock = threading.Lock()
        results = []

        def stealFunction():
            result = buffer.steal()
            with results_lock:
                results.append(result)

        for _ in range(1000):
            handle = threading.Thread(target=stealFunction)
            handle.start()
            handles.append(handle)

        for handle in handles:
            handle.join()
        with results_lock:
            for i in range(1000):
                self.assertTrue(isinstance(results[i], BlockedOrEmpty), "All must be true")

    def test_should_steal_from_full_buffer_from_multiple_threads(self):

        for i in range(10):
            buffer = JobBuffer(1000)
            handles = []
            results_lock = threading.Lock()
            results = []

            def stealFunction():
                result = buffer.steal()
                with results_lock:
                    results.append(result)

            for _ in range(1000):
                job = functools.partialmethod(testJob)
                buffer.push(job)

            for _ in range(1000):
                handle = threading.Thread(target=stealFunction)
                handle.start()
                handles.append(handle)
            for handle in handles:
                handle.join()

            with results_lock:
                successful_steals = 0
                for i in range(1000):
                    self.assertEqual(len(results), 1000)
                    if not isinstance(results[i], Exception):
                        successful_steals += 1
                unsuccessful_steals = 0
                while not isinstance(buffer.wait_and_pop(), Exception):
                    unsuccessful_steals += 1
            self.assertGreater(successful_steals, 950, "Steals not greater than 950")
            self.assertEqual(successful_steals + unsuccessful_steals, 1000)

    def test_should_steal_from_partially_filled_buffer_from_multiple_theads(self):
        buffer = JobBuffer(1000)
        handles = []
        results_lock = threading.Lock()
        results = []

        def stealFunction():
            result = buffer.steal()
            with results_lock:
                results.append(result)

        for _ in range(50):
            job = functools.partialmethod(testJob)
            buffer.push(job)

        for _ in range(1000):
            handle = threading.Thread(target=stealFunction)
            handle.start()
            handles.append(handle)

        for handle in handles:
            handle.join()

        with results_lock:
            successful_steals = 0
            for i in range(1000):
                self.assertEqual(len(results), 1000)
                if not isinstance(results[i], Exception):
                    successful_steals += 1

            unsuccessful_steals = 0
            while not isinstance(buffer.wait_and_pop(), Exception):
                unsuccessful_steals += 1

            self.assertEqual(successful_steals, 50)
            self.assertEqual(unsuccessful_steals, 0)

    def test_should_push_from_one_thread_while_one_is_stealing_on_empty_buffer(self):
        for _ in range(10):
            buffer = JobBuffer(1000)
            push_results_lock = threading.Lock()
            push_results = []
            steal_results_lock = threading.Lock()
            steal_results = []

            def push_thread():
                for _ in range(1000):
                    result = buffer.push(functools.partialmethod(testJob))
                    with push_results_lock:
                        push_results.append(not isinstance(result, Exception))

            push_handle = threading.Thread(target=push_thread)
            push_handle.start()

            def steal_thread():
                for _ in range(1000):
                    result = buffer.steal()
                    with steal_results_lock:
                        steal_results.append(not isinstance(result, Exception))

            steal_handle = threading.Thread(target=steal_thread)
            steal_handle.start()

            push_handle.join()
            steal_handle.join()

            with push_results_lock:
                with steal_results_lock:
                    self.assertEqual(len(push_results), 1000)
                    self.assertEqual(len(steal_results), 1000)

                    successful_pushes = 0
                    successful_steals = 0
                    for i in range(1000):
                        if push_results[i]:
                            successful_pushes += 1
                        if steal_results[i]:
                            successful_steals += 1

                    self.assertGreater(successful_pushes, 950)
                    self.assertGreater(successful_steals, 950)

    def test_should_push_from_one_thread_while_multiple_are_stealing_on_empty_buffer(self):
        for _ in range(10):
            buffer = JobBuffer(1000)
            push_results_lock = threading.Lock()
            push_results = []
            steal_results_lock = threading.Lock()
            steal_results = []

            def push_thread():
                for _ in range(1000):
                    result = buffer.push(functools.partialmethod(testJob))
                    with push_results_lock:
                        push_results.append(not isinstance(result, Exception))

            push_handle = threading.Thread(target=push_thread)
            push_handle.start()

            def steal_thread():
                for _ in range(10):
                    result = buffer.steal()
                    with steal_results_lock:
                        steal_results.append(not isinstance(result, Exception))
            steel_handles = []
            for _ in range(100):
                handle = threading.Thread(target=steal_thread)
                handle.start()
                steel_handles.append(handle)
            push_handle.join()
            for handle in steel_handles:
                handle.join()

            successful_pushes = 0
            successful_steals = 0
            for i in range(1000):
                if push_results[i]:
                    successful_pushes += 1
                if steal_results[i]:
                    successful_steals += 1

            self.assertGreater(successful_pushes, 950)
            self.assertGreater(successful_steals, 950)
