# -*- coding: utf-8 -*-
import time
import traceback
from multiprocessing import Process, Queue


class QPSCounter(object):
    def __init__(self):
        self.success = 0
        self.fail = 0
        self.start_time = time.time()
        self.profile = {}

    def get(self):
        return "{}/{} SuccessRate: {} AvgQPS: {}".format(self.success,
                                                         self.success + self.fail,
                                                         100.0 * self.success / (self.success + self.fail),
                                                         (self.success + self.fail) / (time.time() - self.start_time))

    def qps(self):
        return self.profile.get(int(time.time()), 0)

    def wait(self):
        now = time.time()
        delay = int(now) + 1 - now + 0.01
        # write_log("[{}] Exceed max qps. Sleep: {}".format(current_process().name, delay))
        time.sleep(delay)

    def tick(self, success=True):
        if success:
            self.success += 1
        else:
            self.fail += 1

        now = int(time.time())
        if now not in self.profile:
            self.profile[now] = 1
        else:
            self.profile[now] += 1

        if len(self.profile) > 30:
            for k in sorted(self.profile.keys())[:10]:
                self.profile.pop(k)


def batch_worker(single_worker, task_data_lst, bus, qps_limit, worker_data):
    qps = QPSCounter()
    for this_data in task_data_lst:
        while 0 < qps_limit <= qps.qps():  # wow, amazing Python grammar...  # qps_limit > 0 and qps.qps() >= qps_limit
            qps.wait()
        try:
            result = single_worker(worker_data, this_data)
            qps.tick()
            bus.put((this_data, True, result))
        except Exception:
            qps.tick(False)
            bus.put((this_data, False, traceback.format_exc()))


# Emulates multiprocessing.Queue, ONLY use it in the same process.
class QueueEmulator(object):
    def __init__(self, cb):
        self.cb = cb

    def put(self, data):
        try:
            self.cb(*data)
        except Exception:
            print(traceback.format_exc())


# callback: function(task_data, is_success: bool, result: dict or err: traceback)
#       callback is guaranteed to be executed in the same process and thread as `send_batch_proxy_wsd` caller.
# qps_limit is only meaningful when bigger than 0. Otherwise, it is ignored. (which means no qps limit)
def send_batch_requests(single_worker, worker_data, task_data_lst, callback, qps_limit=-1, max_concurrency=1, rest_interval=0.5):
    if max_concurrency < 1:  # If max_concurrency < 1, it means Request-Handle-Request sync mode.
        print("[WARN] MaxConcurrency<1, using sync request-handle mode.")
        batch_worker(single_worker, task_data_lst, QueueEmulator(callback), qps_limit, worker_data)
    else:
        parr = []
        bus = Queue()
        task_slice = int(len(task_data_lst) / max_concurrency) + 1
        if qps_limit > 0:
            qps_slice = int(float(qps_limit) / max_concurrency)
        else:
            qps_slice = -1
        for i in range(max_concurrency):
            p = Process(target=batch_worker, args=(single_worker, task_data_lst[i*task_slice:(i+1)*task_slice], bus, qps_slice, worker_data))
            parr.append(p)
        for i in range(max_concurrency):
            parr[i].start()
            print("{} started.".format(parr[i].name))

        while True:
            c = 0
            for i in range(max_concurrency):
                if parr[i].is_alive():
                    c += 1

            while not bus.empty():
                try:
                    callback(*bus.get())
                except Exception:
                    print(traceback.format_exc())

            if c < 1:
                break
            else:
                time.sleep(rest_interval)
        for i in range(max_concurrency):
            parr[i].join()


if __name__ == "__main__":
    qps = QPSCounter()
    max_qps = 300
    for i in range(100):
        while qps.qps() >= max_qps:
            qps.wait()
        # send your request here...
        # r = request.post(...)
        qps.tick()  # If failed then call qps.tick(success=False)
