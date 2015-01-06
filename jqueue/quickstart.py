"""
jqueue Quickstart
-----------------

This module provides a function which handles the basics of processing jobs
via jqueue. It turns::
    
    from jqueue import client

    if __name__ == '__main__':
        handler_thread = client.ClientThread(server)
        handler_thread.start()

        while True:
            job = handler_thread.get_job(...)
            if job is None:
                break

            data = job.data
            result = get_result_from_input(data)
            handler_thread.submit_result(result)

        handler_thread.terminate()
        handler_thread.join()

Into::
    
    from jqueue import quickstart

    def handler(job):
        return get_result_from_input(data)
            
    quickstart.process_jobs(server, handler, port=..., ttl=...)
"""
from jqueue import client

def process_jobs(server_host, jobfunc, port=2267, ttl=20):
    """
    Gets jobs from the server, calling a handler with the data from the job, and
    sending the result of the function back to the server.
    """
    handler = client.ClientThread(server_host, port)
    handler.start()
    handler.has_started.wait()

    try:
        while True:
            job = handler.get_job(ttl)
            if job is None:
                break

            result = jobfunc(job.data)
            handler.submit_result(result)
    finally:
        handler.terminate()
        handler.join()
