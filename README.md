These are a few challenging or tricky programming problems I solved in my previous projects.

# 1) Delivery Scheduler

### Problem
Execute all deliveries stored in the Google Firestore collection until their respective end timestamps, ensuring they are processed within a 24-hour window (Service Level Agreement) from their eligible next run timestamps. Implement an efficient solution that scales application resources only when necessary, mitigating sudden spikes in usage.

### Solution
I have implemented scheduler functionality to distribute delivery execution evenly throughout the day. I used the Celery Beat scheduler to trigger this process every 30 minutes. For instance, if we expect to process 2400 deliveries over a 24-hour period, our approach is to execute 50 deliveries every 30 minutes, ensuring a smooth workload distribution throughout the day. This prevents a scenario where all 2400 deliveries are processed in the first 30 minutes, causing a significant spike in resource usage (requiring an upscale of resources by approximately 50 times), leaving no usage for the remainder of the day. The logic for determining which deliveries are eligible for execution in the current batch can be complex, as it involves evaluating the fields `frequency`, `last_run_ts`, and `end_ts`.

### Approach
I used a custom wrapper function `get_all_documents` to retrieve all deliveries, sorting them by the `last_run_ts` key. Subsequently, I filtered the list using the function `is_eligible` to ascertain eligibility for execution in the current batch. The next step involved calculating the number of batches to be processed within the SLA window and determining the batch size based on the expected total executions for that window. Given that the list was already sorted by `last_run_ts`, I iterated through the batch of eligible deliveries and invoked a Celery shared task for each delivery by its unique identifier.

```python
from datetime import datetime, timedelta
from firestore import get_all_documents
from google.api_core.datetime_helpers import DatetimeWithNanoseconds, from_rfc3339

# SLA (Service Level Agreement) duration in minutes
SLA_DURATION_MINUTES = 24 * 60

# Scheduler frequency in minutes
SCHEDULER_FREQUENCY_MINUTES = 30

# Minimum batch size for efficient execution
MIN_BATCH_SIZE = 10

def scheduler():
    """ Executes a batch of eligible deliveries to meet the SLA requirements. """
    # Fetch all deliveries from Firestore sorted by last_run_ts in ascending order
    all_deliveries = get_all_documents(collection="deliveries", sort_keys=["last_run_ts_ascending"])
    eligible_deliveries = filter(is_eligible, all_deliveries)
    delivery_count = len(eligible_deliveries)
    batch_count = SLA_DURATION_MINUTES / SCHEDULER_FREQUENCY_MINUTES
    batch_size = max(MIN_BATCH_SIZE, math.ceil(delivery_count / batch_count))
    batch_size = min(batch_size, delivery_count)
    for delivery in eligible_deliveries[:batch_size]:
        # Call the Celery shared task to execute the delivery by its ID
        execute_delivery.delay(delivery["id"])

def is_eligible(delivery: dict) -> bool:
    """ Determines if the delivery can be executed in the current batch. """
    now = datetime.utcnow()
    end_ts = firestore_ts_to_datetime(delivery["end_ts"])
    if end_ts <= now:
        # The delivery is scheduled to end by the current time
        return False
    if not delivery.get("last_run_ts"):
        # The delivery hasn't been run before; this is the first run
        return True
    last_run_ts = firestore_ts_to_datetime(delivery["last_run_ts"])
    next_run_ts = last_run_ts + timedelta(minutes=delivery["frequency"])
    batch_end_ts = now + timedelta(minutes=SCHEDULER_FREQUENCY_MINUTES)
    return next_run_ts <= batch_end_ts

def firestore_ts_to_datetime(firestore_ts) -> datetime:
    """ Converts a Google Firestore timestamp with nanoseconds into a datetime object. """
    timestamp_rfc3339 = DatetimeWithNanoseconds.rfc3339(firestore_ts)
    timestamp_value = from_rfc3339(timestamp_rfc3339).timestamp()
    return datetime.utcfromtimestamp(int(timestamp_value))
```

# 2) Safe JSON Request  

### Problem
Implement a robust and reliable way to make HTTP requests to external APIs, handle errors gracefully, and log relevant information for monitoring and debugging purposes.

### Solution
I've created a reusable component and integrated it into the company's common framework for all Python web applications. The core of my solution is a wrapper function designed for making HTTP requests. This function accepts essential parameters like the HTTP method (method), URL (url), optional attributes for logging (log_attributes), and additional keyword arguments (**kwargs). To enhance its reliability, I've leveraged the `@retry` decorator from the `tenacity` library. This decorator enables the function to automatically retry the HTTP request using an exponential backoff strategy if it encounters specific exceptions (such as ConnectionError). If these exceptions persist, they are re-raised. Exponential backoff is particularly effective when dealing with distributed services and remote endpoints.

### Approach
The `safe_json_request` function handles different scenarios and returns the HTTP status code and response body. It also keeps a record of the request's timing, status, and payload details, based on whether it succeeds or encounters any issues. The core functionality is encapsulated within the `make_request` function decorated with a retry mechanism, allowing for automatic retries of failed requests with exponential backoff, helping to address temporary network hiccups. The function records the start time of the request and constructs a payload dictionary containing the URL and method. It then makes the HTTP request using the `requests` library including any extra keyword arguments. If a `ConnectionError` occurs during the HTTP request (indicating potential network problems), the function logs the error and re-raises the same exception. This ensures that network-related issues are retried. If the HTTP response status code is 400 or higher, indicating a client or server error, the function constructs a JSON response containing the status code and the formatted response body. It then logs this error and raises an HTTPError with the JSON response as an argument. If the HTTP request is successful (status code less than 400), the function logs the success and returns the response object. The code provides extensive logging of request details, errors, and outcomes, making it easier to diagnose and troubleshoot issues in a production environment.

```python
import json
import logging
import requests
from datetime import datetime
from requests.exceptions import HTTPError, ConnectionError
from tenacity import retry, stop_after_attempt, wait_random_exponential


def safe_json_request(method, url, log_attributes=None, **kwargs):
    """ Convenience function for making HTTP requests to external APIs with error handling.
    :param method: HTTP method (GET, POST, PUT, etc.)
    :param url: Request URL.
    :param url: Attributes to be logged.
    :param kwargs: Additional parameters.
    :return: Tuple of status_code and JSON body as a Python dictionary. """
    HTTP_FAILURE_CODE = {400: 'Bad Request', 401: 'Authentication Error', 403: 'Authentication Error', 404: 'Not Found'}
    LOG_LEVEL = {'critical': 50, 'error': 40, 'warning': 30, 'info': 20, 'debug': 10}
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger(__name__)
    if log_attributes is None:
        log_attributes = {}

    def format_response_body(response):
        try:
            return response.json()
        except ValueError:
            return {'content': response.text}
    
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_random_exponential(multiplier=0.5, max=3))
    def make_request():
        start_time = datetime.utcnow().timestamp()
        payload = {'url': url, 'method': method}
        try:
            response = requests.request(method=method, url=url, **kwargs)
        except ConnectionError as e:
            log_message = {'process_start_utc': start_time, 'process_end_utc': datetime.utcnow().timestamp(), 
                           'payload': payload, 'attributes': log_attributes, 'process_type': 'http_request', 
                           'status': 'failure', 'failure_classification': "Connection Error"}
            logger.log(level=LOG_LEVEL['info'], msg=json.dumps(log_message))
            raise e
        else:
            payload['status_code'] = response.status_code
            if response.status_code >= 400:
                response_json = {'status_code': response.status_code, 'response': format_response_body(response=response)}
                payload['response'] = format_response_body(response=response)
                log_message = {'process_start_utc': start_time, 'process_end_utc': datetime.utcnow().timestamp(), 
                               'payload': payload, 'attributes': log_attributes, 'process_type': 'request', 'status': 'failure', 
                               'failure_classification': HTTP_FAILURE_CODE.get(response.status_code, "Other Error")}
                logger.log(level=LOG_LEVEL['info'], msg=json.dumps(log_message))
                raise HTTPError(json.dumps(response_json))
            else:
                log_message = {'process_start_utc': start_time, 'process_end_utc': datetime.utcnow().timestamp(), 
                               'payload': payload, 'attributes': log_attributes, 'process_type': 'request', 
                               'status': 'success', 'failure_classification': ''}
                logger.log(level=LOG_LEVEL['info'], msg=json.dumps(log_message))
            return response

    try:
        response = make_request()
    except ConnectionError:
        status_code = None
        response_json = {}
    except HTTPError as exc:
        res = json.loads(exc.args[0])
        status_code = res['status_code']
        response_json = res['response']
    else:
        status_code = response.status_code
        response_json = format_response_body(response=response)
    return status_code, response_json
```

# 3) Validate Parameterized SQL Query

### Problem
Within an application meant for registering and executing custom parameterized queries, as provided by the Data Science team, design and implement an effective process for handling query details received through API requests. The aim is to guarantee that these queries follow specific guidelines and that the supplied parameters are both valid and properly structured. Ultimately, the goal is to transform these queries into valid parameterized BigQuery SQL queries.

### Solution
I implemented a function with the purpose of validating SQL queries and their associated parameters. If they pass validation, the function transforms both the query and parameters to adhere to BigQuery's parameterized query standards. This function conducts checks to identify any invalid SQL keywords within the query, validates the provided parameters, and adjusts the query if it includes array-type parameters. It also confirms the uniqueness of parameter names and verifies whether sample values match the specified data types. In the end, it provides the transformed query and a list of query parameters ready for execution.

### Approach
The `validate_and_transform_query` function takes as input a parameterized SQL query (query) and a list of parameter details (parameters). It checks if the query contains any of the predefined invalid SQL keywords (case-insensitive), if found returns an error message to ensure that the query does not perform dangerous operations. For each parameter, the function validates whether the parameter name is present in the query, the parameter name is unique among all parameters and the parameter's sample value(s) match the specified data type. If a parameter is defined as an array type (e.g., UNNEST(@parameter)), the function transforms the query to ensure correct syntax. It replaces variations of 'UNNEST (@parameter)' with 'UNNEST(@parameter)' to ensure consistency. It also checks if the array-type parameter reference is syntactically correct. For each valid parameter, the function creates a BigQuery query parameter (query_param) with the specified name, data type, and sample value(s). It distinguishes between scalar and array parameters based on the sample value type. At each step, if any validation or transformation fails, the function returns an error message. If there are duplicate parameter names, invalid data types, missing parameter references, or an invalid sample format, the corresponding error message is returned. If all validations are successful, the function returns the transformed query, a list of query parameters, and None for the error. The caller can then use the transformed query and parameters for further processing, such as executing the query against a BigQuery dataset.

```python
from datetime import date, datetime, time
from google.cloud import bigquery

# SQL keywords that are not allowed in the query
INVALID_SQL_KEYWORDS = ['DROP', 'DELETE', 'UPDATE', 'REPLACE', 'ALTER']

# Supported BigQuery data types and their corresponding Python types
BQ_DATA_TYPES = {
    'INT64': int, 'NUMERIC': float, 'FLOAT64': float, 'BOOL': bool, 'STRING': str, 'BYTES': int, 
    'DATE': date, 'DATETIME': datetime, 'GEOGRAPHY': str, 'TIME': time, 'TIMESTAMP': datetime
}

def validate_and_transform_query(query: str, parameters: list):
    """ Validate and transform a parameterized BigQuery SQL query.
    :param query (str): The parameterized SQL query to validate and transform.
    :param parameters (list): A list of dictionaries containing parameter details.
        Each dictionary should have 'name', 'type', and 'sample' keys.
    :return: Tuple containing the transformed query (str), query parameters (list), 
        and an error message (str) if any validation fails otherwise None. """
    # Check for invalid SQL keywords in the query
    if any(keyword.lower() + ' ' in query.lower() for keyword in INVALID_SQL_KEYWORDS):
        return query, f"Query statement cannot have the SQL keywords: {', '.join(INVALID_SQL_KEYWORDS)}"
    query_params = []
    names_appeared = set()
    error = None
    for parameter in parameters:
        name = parameter.get('name')
        data_type = parameter.get('type')
        sample = parameter.get('sample')
        # Check if the parameter name is present in the query
        if '@{}'.format(name) not in query:
            return query, [], "One or more parameters defined are not used in the query statement."
        if isinstance(sample, list):
            # Modify the query if it refers to an array type parameter
            count_unnest = query.count('UNNEST(@{})'.format(name))
            count_param = query.count('@{}'.format(name))
            if count_unnest != count_param:
                query = query.replace('UNNEST (@{})'.format(name), 'UNNEST(@{})'.format(name))
                query = query.replace('UNNEST(@{})'.format(name), '@{}'.format(name), count_unnest - 1)
                if count_unnest != count_param:
                    return query, [], "Please make sure that @{} parameter reference is syntactically correct".format(name)
        bq_type = BQ_DATA_TYPES.get(data_type)
        if bq_type is None:
            return query, [], f"'{data_type}' is not one of {list(BQ_DATA_TYPES.keys())}"
        # set query_param while checking if the sample value (or array of values) matches with a bq data type
        if isinstance(sample, bq_type):
            query_param = bigquery.ScalarQueryParameter(name, data_type, sample)
        elif isinstance(sample, list) and all(isinstance(value, bq_type) for value in sample):
            query_param = bigquery.ArrayQueryParameter(name, data_type, sample)
        else:
            return query, [], "Invalid format of the parameter sample value"
        # Check for duplicate parameter names
        if name in names_appeared:
            return query, [], "Duplicate parameter details found"
        names_appeared.add(name)
        query_params.append(query_param)
    # Check if the number of query parameters matches the number of provided parameters
    if len(query_params) != len(parameters):
        return query, [], "Invalid query parameters"
    return query, query_params, error
```
