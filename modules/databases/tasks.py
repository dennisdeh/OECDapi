from celery import Celery, Task
from celery.exceptions import MaxRetriesExceededError
import requests
from requests.exceptions import HTTPError, Timeout
import sdmx
import os

# Instantiate Celery app
redis_port = os.getenv("REDIS_PORT_HOST")
redis_container_name = os.getenv("REDIS_CONT_NAME")
if redis_port is None or redis_container_name is None:
    raise AssertionError("Environment variables for the redis container are not found")

app = Celery(
    "tasks",
    broker=f"redis://localhost:{redis_port}",
    backend=f"redis://localhost:{redis_port}",
)
app.conf.event_serializer = "pickle"
app.conf.task_serializer = "pickle"
app.conf.result_serializer = "pickle"
app.conf.accept_content = ["application/json", "application/x-python-serialize"]


class BaseTaskWithRetry(Task):
    """
    BaseTaskWithRetry class provides a task with retry capabilities designed to handle specific exceptions
    during task execution. The class utilizes the Celery task retry mechanism to handle failures caused by
    HTTP or network-related issues.

    This class is specifically designed for tasks that may encounter transient issues and ensures
    that errors such as connection errors or timeouts are retried according to defined configurations.

    :param autoretry_for: Tuple of exception types for which the task will automatically retry.
    :param retry_kwargs: Dictionary containing retry configuration such as the maximum number of retries and countdown.
    :param retry_backoff: Boolean indicating whether to apply exponential backoff between retries.
    """
    autoretry_for = (
        requests.RequestException,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectionError,
    )
    retry_kwargs = {"max_retries": 1, "countdown": 1}
    retry_backoff = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Handle failure by raising the custom InvestioException.
        """
        if (
            isinstance(exc, requests.HTTPError)
            or isinstance(exc, requests.RequestException)
            or isinstance(exc, requests.exceptions.ReadTimeout)
            or isinstance(exc, requests.exceptions.ConnectionError)
        ):
            try:
                self.retry()
            except MaxRetriesExceededError:
                raise MaxRetriesExceededError(
                    f"Task {task_id} failed after max retries: {str(exc)}"
                )
        else:
            pass


# %% Tasks
@app.task(
    bind=True,
    rate_limit="300/m",
    base=BaseTaskWithRetry,
    throws=(
        requests.HTTPError,
        requests.RequestException,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectionError,
    ),
)
def json_request_celery(self, url: str, timeout: int = 2):
    """
    Handles the execution of a Celery task that performs an HTTP GET request to
    fetch JSON data from a given URL. The task retries upon specific request
    exceptions and returns data if the response contains non-empty JSON.
    If the JSON response is empty or unreachable after maximum retries, None
    is returned.

    :param self: Celery task instance for method binding and retrying logic.
    :param url: The URL endpoint to fetch JSON data from.
    :type url: str
    :param timeout: The time (in seconds) to wait before giving up on the HTTP request.
    :type timeout: int
    :return: Parsed JSON response if successful and non-empty, otherwise None.
    :rtype: dict, list, None

    :raises requests.HTTPError: Raised for HTTP specific connection errors.
    :raises requests.RequestException: Raised for any general request-related exceptions.
    :raises requests.exceptions.ReadTimeout: Raised when HTTP request times out.
    :raises requests.exceptions.ConnectionError: Raised when a connection error occurs.
    """
    # try to get data
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an HTTPError for bad status codes
        data = response.json()
        # check on what was returned
        if (isinstance(data, list) or isinstance(data, dict)) and len(data) == 0:
            return None
        return data

    except (
        requests.HTTPError,
        requests.RequestException,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectionError,
    ) as e:
        self.retry(exc=e)
    except MaxRetriesExceededError:
        return None
    except Exception:
        return None


@app.task(
    bind=True,
    rate_limit="100/m",
    throws=MaxRetriesExceededError,
)
def sdmx_request_celery(self, url):
    """
    Executes an SDMX request using a Celery task. This function processes the given
    URL to retrieve data through the SDMX API and handles potential HTTP errors
    and timeouts by retrying the request with exponential backoff. If response
    status is unauthorized, forbidden, not found, or non-successful, it raises
    a custom exception indicating that the requested data table was not found.

    :param self: Celery task instance
    :param url: URL string of the SDMX API endpoint
    :return: The data response retrieved from the SDMX API or None on retryable
        error

    :raises MaxRetriesExceededError: Raised when the request exceeds the
        maximum retry attempts or when the requested resource is invalid or
        unavailable
    """
    # try to get data
    try:
        response = sdmx.read_url(url)
    except (HTTPError, Timeout) as e:
        # Handle HTTP and timeout exceptions
        (
            self.retry(exc=e, countdown=2**self.request.retries)
            if self.request.retries < self.max_retries
            else None
        )
        return None

    # checks for errors
    if not isinstance(response, list) and response.response.status_code in [403, 404]:
        raise MaxRetriesExceededError("Data table not found with the API.")
    elif not isinstance(response, list) and response.response.status_code != 200:
        raise MaxRetriesExceededError("Data table not found with the API.")
    return response

