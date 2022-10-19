from queue import Empty
from threading import Thread
from subprocess import Popen, PIPE, run

import time
import getpass

import logging

class RemoteExecutor:
    def __init__(self, task_queue, hosts):
        self.task_queue = task_queue
        self.hosts = hosts
        self.workers = None
        self.initialize_workers()

    def initialize_workers(self):
        self.workers = [SshWorker(host, self.task_queue) for host in self.hosts]
        logging.debug(f"initialized workers:\n{[x.host.hostname for x in self.workers]}")

    def start(self):
        for worker in self.workers:
            worker.start()
            logging.debug(f"started worker {worker.host.hostname}")

    def wait(self):
        for worker in self.workers:
            logging.debug(f"waiting for {worker.host.hostname}")
            worker.join()
            logging.debug(f"joined {worker.host.hostname}")


class SshWorker(Thread):
    def __init__(self, host, task_queue):
        super().__init__()
        self.task_queue = task_queue
        self.host = host

    def run(self):
        while True:
            try:
                internal_task = self.task_queue.get(block=False)
            except Empty:
                break

            execute_remote_task(self.host, internal_task)


def execute_remote_task(host, internal_task):
    task = internal_task.task
    internal_task.future.set_running_or_notify_cancel()

    task_memory_limit = 7 * 1024 * 1024 * 1024  # 7 GB
    if task.memory_limit is not None:
        task_memory_limit = task.memory_limit * 1024 * 1024 * 1024

    try:
        start = time.time()

        if host.username is None:
            host.username = getpass.getuser()

        auth = host.username + "@" + host.hostname + " -p " + str(host.port)
        logging.debug("going to Popen: " + "exec ssh " + auth + " " + task.command)
        process = Popen("exec ssh " + auth + " " + task.command,
                        stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)

        if task.input is not None:
            process.stdin.write(task.input)

        while process.poll() is None:
            mem_proc = Popen("ssh " + auth + " " + host.mem_check,
                             stdout=PIPE, shell=True)

            mem_stdout, mem_stderr = mem_proc.communicate()

            mem_output = mem_stdout.decode('utf-8')

            mem_used = int(mem_output)
            logging.debug(f"mem_used: {mem_used}")

            mem_proc.stdout.close()
            if mem_used >= task_memory_limit:
                internal_task.task.output = "OutOfMemory exceeded max_bytes: " + str(mem_used)
                process.terminate()
                process.kill()
            if (time.time() - start) > task.time_limit:
                internal_task.task.output = "OutOfTime exceeded time_limit: " + str(task.time_limit)
                process.terminate()
                process.kill()

        process.wait()

        # if process.returncode == 0:
        #     task.output = b''.join(process.stdout.readlines())
        logging.debug(f"procces return code: {process.returncode}")
        task.output = b''.join(process.stdout.readlines())
        logging.debug("task output:")
        logging.debug(task.output)

        task.error = b''.join(process.stderr.readlines())

        internal_task.future.set_result(task)

        process.stdin.close()
        process.stdout.close()
        process.stderr.close()
    except Exception as e:
        internal_task.future.set_exception(e)
