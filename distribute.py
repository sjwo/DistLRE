#!/usr/bin/env python

from distlre.distlre import DistLRE, Task, RemoteHost
import logging
from logging import critical, error, warning, info, debug
import datetime
LOG_MESSAGE_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"

def main():
    configure_logging("debug")
    log.info("distribute running")
    # rh = RemoteHost(...)
    executor = DistLRE(
        remote_hosts=[
            rh
        ],
        local_threads=0,
    )
    task = Task(command="ls ~", meta="META", time_limit=10, memory_limit=10)
    future = executor.submit(task)
    executor.execute_tasks()
    executor.wait()
    print(future.result().output)

def configure_logging(log_level):
    log_level = log_level.upper()
    global log
    log = logging.getLogger()#__name__)
    log.setLevel(log_level)
 
    formatter = logging.Formatter(LOG_MESSAGE_FORMAT)
 
    # output log message to stdout
    sh = logging.StreamHandler()
    sh.setLevel(log_level)
    sh.setFormatter(formatter)
 
    log.addHandler(sh)
 
if __name__ == "__main__":
    main()
