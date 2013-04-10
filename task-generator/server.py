# -*- coding: utf-8 -*-
import sys
import SocketServer
import threading
import Queue
import logging
import time
from factory import TaskDiskFactory, JobFactory
from exception import TaskNotIterableError

logging.basicConfig(level=logging.DEBUG, format='%(name)s: %(message)s',)


class TaskManagerTCPHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        self.logger = logging.getLogger("TaskManagerTCPHandler")

        self.data = self.request.recv(1024).strip()
        self.logger.debug("{0} wrote: {1}".format(self.client_address[0], self.data))

        try:
            cmd = getattr(self.server, self.data)
            self.request.send(cmd())
        except AttributeError:
            self.request.sendall("Command not supported.")



class TaskManagerServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, server_address,
                 handler_class=TaskManagerTCPHandler,
                 task_factory=TaskDiskFactory,
                 job_factory=JobFactory):
        SocketServer.TCPServer.__init__(self, server_address, handler_class)
        self.logger = logging.getLogger('TaskManagerServer')
        self.manager = None
        self.running = False
        self.tfactory = task_factory
        self.jfactory = job_factory
        self.queue = Queue.Queue()
        self.lock = threading.Lock()

    def start(self):
        if not self.is_running():
            self.running = True
            self.manager = threading.Thread(target=self.worker)
            self.manager.setDaemon(True)
            self.manager.start()

            msg = "Task manager started."
            self.logger.info(msg)
            return msg
        else:
            msg = "Task thread is running."
            self.logger.warning(msg)
            return msg

    def stop(self):
        self.running = False
        return "Task manager worker stopped."

    def worker(self):
        while self.running:
            try:
                for task in self.tfactory():
                    for job in self.jfactory(task):
                        self.queue.put(job)
                        self.logger.info("Job {0} enqueued.".format(job))

            except TaskNotIterableError:
                self.logger.warning("Task dropped, not iterable.")
            finally:
                self.logger.info('Nothing to do, I go to sleep a while.')
                time.sleep(1)

        self.logger.info("Task generator worker stopped.")

    def is_running(self):
        return self.running

    def kill(self):
        try:
            self.stop()
            return "Server shuting down ..."
        finally:
            self.shutdown()
            self.logger.info("Server shuting down ...")

    def get(self):
        try:
            with self.lock:
                elem = self.queue.get()
                return str(elem)
        except:
            self.queue.put(elem)
            self.logger.warning("Task enqueue again, exception thown.")
        finally:
            self.queue.task_done()

    def status(self):
        return "QS:{0}".format(self.queue.qsize())

if __name__ == "__main__":
    logger = logging.getLogger("Server")
    logger.info("Server starting...")

    try:
        HOST, PORT = "0.0.0.0", 9999

        # Create the server, binding to localhost on port 9999
        server = TaskManagerServer((HOST, PORT), TaskManagerTCPHandler, task_factory=TaskDiskFactory,)
        server.start()

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        logger.info("Server shuting down ...")
        sys.exit(0)

