import concurrent.futures
import logging
from uuid import uuid4

import timeout_decorator
from flask import (
    current_app,
    g,
)

from c_service import config
from c_service.clients.aws import AwsSqsClient
from c_service.processors.notification_consumers.q1 import consume_q1_notifications
from c_service.processors.notification_consumers.q2 import consume_q2_notifications
from c_service.processors.notification_consumers.q3 import consume_q3_notifications
from c_service.processors.notification_consumers.q4 import consume_q4_notifications
from c_service.processors.notification_consumers.q5 import consume_q5_notifications
from c_service.processors.notification_consumers.q6 import consume_q6_notifications
from c_service.processors.notification_consumers.internal import consume_internal_notifications

from c_service.utils import log_timeout_error
from c_service.wsgi import app

try:
    import uwsgi
except ImportError:
    UWSGI_ENABLED = False
else:
    UWSGI_ENABLED = True

logger = logging.getLogger(__name__)


def dispatch(signal):
    consumer_config = CONSUMERS_CONFIG.get(signal)
    queue_url = consumer_config["queue"]
    consumer_func = consumer_config["consumer_func"]
    batch_size = consumer_config["batch_size"]

    with app.app_context():
        g.operation_id = uuid4()
        sqs_client = AwsSqsClient(queue_url)
        messages = sqs_client.fetch_messages_batch(batch_size=batch_size)

        if not messages:
            return

        dispatch_messages(
            consumer_func=consumer_func, sqs_client=sqs_client, messages=messages
        )


@log_timeout_error("consumer_func")
@timeout_decorator.timeout(2 * config.EXTERNAL_SERVICE_RESPONSE_TIMEOUT)
def dispatch_messages(*_, consumer_func, sqs_client, messages):  # noqa: U101
    with concurrent.futures.ThreadPoolExecutor() as executor:
        promises = {}
        for message_handle, message_body in messages.items():
            promise = executor.submit(
                _call_from_different_thread,
                current_app.app_context().app,
                consumer_func,
                message_body,
            )

            promises[promise] = message_handle

        message_handles = []
        msg_id = 0
        for promise in concurrent.futures.as_completed(promises):
            if promise.result():
                message_handles.append(
                    {"Id": f"id-{msg_id}", "ReceiptHandle": promises[promise]},
                )
                msg_id += 1

        if message_handles:
            sqs_client.delete_messages_batch(message_handles)


def _call_from_different_thread(flask_app, fn, *args):
    with flask_app.app_context():
        g.operation_id = uuid4()
        return fn(*args)


CONSUMERS_CONFIG = {
    10: {
        "enabled": config.Q1_POLLING_ENABLED,
        "batch_size": config.Q1_POLLING_BATCH_SIZE,
        "time_interval": config.Q1_POLLING_INTERVAL,
        "consumer_func": consume_q1_notifications,
        "queue": config.Q1_SQS_URL,
    },
    20: {
        "enabled": config.Q2_POLLING_ENABLED,
        "batch_size": config.Q2_POLLING_BATCH_SIZE,
        "time_interval": config.Q2_POLLING_INTERVAL,
        "consumer_func": consume_q2_notifications,
        "queue": config.Q2_SQS_URL,
    },
    30: {
        "enabled": config.Q3_POLLING_ENABLED,
        "batch_size": config.Q3_POLLING_BATCH_SIZE,
        "time_interval": config.Q3_POLLING_INTERVAL,
        "consumer_func": consume_q3_notifications,
        "queue": config.Q3_SQS_URL,
    },
    40: {
        "enabled": config.Q4_POLLING_ENABLED,
        "batch_size": config.Q4_POLLING_BATCH_SIZE,
        "time_interval": config.Q4_POLLING_INTERVAL,
        "consumer_func": consume_q4_notifications,
        "queue": config.Q4_SQS_URL,
    },
    50: {
        "enabled": config.INTERNAL_POLLING_ENABLED,
        "batch_size": config.INTERNAL_POLLING_BATCH_SIZE,
        "time_interval": config.INTERNAL_POLLING_INTERVAL,
        "consumer_func": consume_internal_notifications,
        "queue": config.INTERNAL_SQS_URL,
    },
    60: {
        "enabled": config.Q5_POLLING_ENABLED,
        "batch_size": config.Q5_POLLING_BATCH_SIZE,
        "time_interval": config.Q5_POLLING_INTERVAL,
        "consumer_func": consume_q5_notifications,
        "queue": config.Q5_SQS_URL,
    },
    70: {
        "enabled": config.Q6_POLLING_ENABLED,
        "batch_size": config.Q6_POLLING_BATCH_SIZE,
        "time_interval": config.Q6_POLLING_INTERVAL,
        "consumer_func": consume_q6_notifications,
        "queue": config.Q6_SQS_URL,
    },
}

if UWSGI_ENABLED:
    for signal_number, conf in CONSUMERS_CONFIG.items():
        if conf["enabled"] is True:
            uwsgi.register_signal(signal_number, "", dispatch)
            uwsgi.add_timer(signal_number, conf["time_interval"])
