import shutil
import subprocess
import importlib
import os
import sys
import platform
import time
from typing import Union
from modules.databases.redis_initialise import initialise_redis_backend
from rich.progress import Progress


def celery_app_initialise(tasks_module: str, path_db_env: str):
    """
    Initialise redis backend, import tasks, and initialise the Celery app.
    """
    # initialise redis
    initialise_redis_backend(path_dotenv_file=path_db_env)

    # import from tasks functions to use from the given module
    return importlib.import_module(tasks_module)


def celery_workers_start(
    tasks,
    task_queuing: str,
    pool: Union[str, None] = None,
    concurrency: int = 100,
):
    """
    Start Celery workers; pooling and concurrency settings can be controlled.
    """
    if task_queuing == "celery_wait" or task_queuing == "celery_submit":
        assert tasks is not None, "tasks module must be imported first"
        assert isinstance(concurrency, int), "concurrency must be an integer"
        # set the default pool-type depending on the OS
        if pool is None:
            if platform.system() == "Linux":
                pool = "gevent"
            elif platform.system() == "Windows":
                pool = "threads"
            else:
                raise NotImplementedError(
                    f"Not implemented for this OS (platform.system(): {platform.system()})"
                )
        else:
            assert isinstance(pool, str), "pool must be a string"
            assert pool in ["solo", "threads", "gevent", "eventlet"]
        # Use the Python executable from the active environment to invoke Celery
        python_executable = sys.executable  # Gets the current Python executable in use
        celery_entry_point = [
            python_executable,
            "-m",
            "celery",
            "-A",
            str(tasks).split("'")[1],
            "--workdir",
            os.path.abspath(os.getcwd()),
            "-q",
            "worker",
            f"--pool={pool}",
            f"--concurrency={concurrency}",
            "broker_connection_retry_on_startup=True",
        ]
        try:
            # Start the Celery worker process
            return subprocess.Popen(celery_entry_point)
        except FileNotFoundError:
            raise FileNotFoundError(
                "Celery could not be started. Ensure it is installed in your conda environment "
                "and accessible via the active Python interpreter."
            )
        except Exception as e:
            raise RuntimeError(f"Failed to start Celery workers: {e}")

    else:
        print("Task queuing is not using Celery, invocation ignored")


def celery_workers_stop(worker_processes, task_queuing: str):
    """
    Stop all Celery workers.
    """
    if task_queuing == "celery_wait" or task_queuing == "celery_submit":
        worker_processes.terminate()
        worker_processes.kill()
    else:
        print("Task queuing is not using Celery, invocation ignored")


def celery_workers_running(worker_processes):
    """
    Check if the Celery workers are running
    """
    try:
        return worker_processes is not None and worker_processes.returncode is None
    except AttributeError:
        return False


def celery_download_status(d: dict, combine_level0: bool = False):
    """
    Get the status of a number of submitted tasks encoded
    in AsyncResult objects.

    If combine_level0, then the dictionaries at level 0 will
    be combined to form a new dictionary with all level 1 dictionaries.

    Must have the structure (also after combination):
    {sheet: {symbol: AsyncResult, ...}, ...}
    """
    # optional combination of level 0 dictionaries
    if combine_level0:
        d_level1 = {}
        for k, v in d.items():
            d_level1 = {**d_level1, **v}
        d = d_level1
    # 1: monitor progress until all have been downloaded
    done = False
    d_progress = {}
    time_start = time.time()
    print(" *** Downloading data *** ")
    with Progress() as progress:
        # add progress bars
        for sheet in d:
            d_progress[sheet] = progress.add_task(f"[red]{sheet}", total=len(d[sheet]))
        while not done:
            done = True
            d_check = {}
            for sheet in d:
                n = 0
                for symbol in d[sheet]:
                    if d[sheet][symbol].ready():
                        n += 1
                progress.update(d_progress[sheet], completed=n)
                if n == len(d[sheet]):
                    d_check[sheet] = True
                else:
                    d_check[sheet] = False

            for _, status in d_check.items():
                done = done * status
    time_end = time.time()
    print(f"Completed! (in {round((time_end - time_start) / 60, 2)}m)")
