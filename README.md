These are a few challenging or tricky programming problems I solved in my previous projects.

# Delivery Scheduler

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
