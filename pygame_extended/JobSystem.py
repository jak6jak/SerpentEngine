import multiprocessing
import os
import time
from ctypes import c_bool
from dataclasses import dataclass
from pygame_extended.JobBuffer import JobBuffer, IsEmpty, BlockedOrEmpty, BlockedOrFull
from chemical import take, it, Skip


@dataclass
class WorkerThread:
    local_buffer: JobBuffer
    steal_buffers: list
    index: int

    def __post_init__(self):
        self.steal_buffers_lock = multiprocessing.Lock()


WORKER_THREAD: WorkerThread = None


def thread_start(core_ids, buffers, i, done):
    print("running thread start")
    _setup_worker_thread(core_ids, buffers, i)
    _run_worker_thread(i, done)


def init(buffer_capacity, threads):
    # estimate workthreads adn according affinities
    cpu_count = os.cpu_count()
    threads = min(cpu_count, threads)

    affinities = []
    for _ in range(threads):
        affinities.append([])
    for i in range(cpu_count):
        affinities[i % threads].append(i)
    done = multiprocessing.Value(c_bool, False)
    # setup job buffers
    buffers = []
    for _ in range(threads):
        buffers.append(JobBuffer(buffer_capacity))

    handles = []

    core_ids = it(affinities).take(threads).skip(1).collect()
    for i, core_id in enumerate(core_ids):
        #
        p = multiprocessing.Process(target=thread_start, daemon=False, args=(core_id, buffers, i, done,))
        p.start()
        handles.append(p)
    print("spawned", len(handles), "additional worker threads")
    time.sleep(1)
    core_ids = affinities[0]

    _setup_worker_thread(core_ids, buffers, 0)
    return JobSystemGuard(handles, done)


def thread_index():
    result = -1
    if WORKER_THREAD is not None:
        result = WORKER_THREAD.index
    else:
        raise Exception("Calling thread isn't a worker thread")
    return result


def submit(job):
    not_pushed = None
    global WORKER_THREAD

    p = multiprocessing.Process(target=job)

    if WORKER_THREAD is not None:
        push_result = WORKER_THREAD.local_buffer.push(p)
        if isinstance(push_result, BlockedOrFull):
            not_pushed = push_result.not_pushed
    else:
        raise Exception("couldn't submit job, calling thread isn't a worker thread")

    if not_pushed is not None:
        not_pushed.start()

    return p


def _setup_worker_thread(core_ids, buffers, index):
    # TODO affinity for cpu
    global WORKER_THREAD
    local_buffer = buffers[index]
    steal_buffers = []

    for i, buffer in enumerate(buffers):
        if i <= index + 1:
            continue
        steal_buffers.append(buffer)

    b = it(buffers).take(index).collect()
    steal_buffers.append(b[len(b):])
    WORKER_THREAD = WorkerThread(local_buffer, steal_buffers, index)


def _run_worker_thread(i, done):
    while not done.value:
        run_pending_job()
    _empty_buffer(i)


def run_pending_job():
    result = _pop_job()
    if isinstance(result, IsEmpty):

        steal = _steal_job()
        if isinstance(steal, BlockedOrEmpty):
            yield
        else:
            steal.start()
    else:
        result()


@dataclass
class JobSystemGuard:
    handles: list
    done: multiprocessing.Value

    def __del__(self):
        print("dropping job system")
        self.done.value = True

        _empty_buffer(0)

        if self.handles is not None:
            i = 0
            for handle in self.handles:
                i += 1
                result = handle.join()
                if handle.exitcode == 0:
                    print("joined thread", i)
                else:
                    print("failed to join thread:", i, "results:", result)
            self.handles = None
        else:
            print("handles already joined")
        print("job system finished")


def _empty_buffer(index):
    while True:
        print("emptying", index)
        job = _pop_job()
        if isinstance(job, IsEmpty):
            break
        job.start()


def _pop_job():
    result = IsEmpty()
    global WORKER_THREAD
    if WORKER_THREAD is not None:
        return WORKER_THREAD.local_buffer.wait_and_pop()
    else:
        raise Exception("couldn't pop job, calling thread wasn't a worker thread")


def _steal_job():
    result = BlockedOrEmpty()
    global WORKER_THREAD
    if WORKER_THREAD is not None:
        with WORKER_THREAD.steal_buffers_lock:
            for buffer in WORKER_THREAD.steal_buffers:
                result = buffer.steal()
                if not isinstance(result, Exception):
                    break

    else:
        raise Exception("couldn't steal a job, calling thread isn't a worker thread")

    return result


if __name__ == '__main__':
    job_system = init(10, 100)

    del job_system
