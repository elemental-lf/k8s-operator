import concurrent
import logging
import signal
import sys
import time
from collections import namedtuple
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue, Empty
from threading import Event
from typing import List, Dict, NoReturn, Tuple

import kubernetes
import blinker
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError

FINALIZER_NAME = 'ElementalK8sOperatorDelete'
MAX_WORKERS = 10
WATCH_TIMEOUT = 10
QUEUE_TIMEOUT = 10
CRD_NOT_FOUND_RETRY_INTERVAL = 5

signal_apply = blinker.signal('elemental.k8s_operator.apply')
signal_delete = blinker.signal('elemental.k8s_operator.delete')

CustomResource = namedtuple('CustomResource', ['group', 'version', 'resource'])

custom_resources: List[CustomResource] = []
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
operator_shutdown = Event()

logger = logging.getLogger()

try:
    kubernetes.config.load_incluster_config()
    logger.debug('Configured in cluster with service account.')
except Exception:
    try:
        kubernetes.config.load_kube_config()
        logger.debug('Configured via kubeconfig file.')
    except Exception:
        raise RuntimeError('No Kubernetes configuration found.')


def _logger_prefix_from_resource_object(resource_object: Dict) -> str:
    namespace = resource_object['metadata']['namespace']
    name = resource_object['metadata']['name']
    uid = resource_object['metadata']['uid']
    return f'{namespace}/{name} ({uid})'


def _handle_custom_resource_change(custom_resource: CustomResource, resource_object: Dict) -> None:
    custom_objects_api_instance = kubernetes.client.CustomObjectsApi()
    logger_prefix = _logger_prefix_from_resource_object(resource_object)

    group = custom_resource.group
    version = custom_resource.version
    resource = custom_resource.resource

    namespace = resource_object['metadata']['namespace']
    name = resource_object['metadata']['name']

    resource_object['metadata'].setdefault('finalizers', [])
    patch_object = {}
    if signal_delete.has_receivers_for(custom_resource) and resource_object['metadata'].get('deletionTimestamp', None) is not None:
        logger.info(f'{logger_prefix} marked for deletion')

        if FINALIZER_NAME not in resource_object['metadata']['finalizers']:
            # We've already washed our hands of this one
            return

        try:
            if signal_delete.receivers:
                logger.debug(f'{logger_prefix} calling delete.')
                returns = signal_delete.send(custom_resource, resource_object=resource_object)
                if len(returns) > 1:
                    logger.warning(f'Signal {signal_delete.name} sent by {custom_resource} had more than one receiver. Ignoring all return values but the first.')
                patch_object['status'] = returns[0][1]
        except Exception as exception:
            logger.error(f'{logger_prefix} delete failed with {exception.__class__.__name__} exception: {exception}')
            return
        else:
            if not patch_object['status']:
                logger.debug(f'{logger_prefix} delete returned empty status, removing finalizer.')
                patch_object['metadata'] = {
                    'finalizers': list(filter(lambda f: f != FINALIZER_NAME, resource_object['metadata']['finalizers']))
                }
    else:
        logger.info(f'{logger_prefix} triggered by change.')

        if not signal_delete.has_receivers_for(custom_resource) or FINALIZER_NAME in resource_object['metadata']['finalizers']:
            logger.debug(f'{logger_prefix} calling apply.')
            try:
                if signal_apply.receivers:
                    returns = signal_apply.send(custom_resource, resource_object=resource_object)
                    if len(returns) > 1:
                        logger.warning(f'Signal {signal_apply.name} sent by {custom_resource} had more than one receiver. Ignoring all return values but the first.')
                    patch_object['status'] = returns[0][1]
            except Exception as exception:
                logger.error(f'{logger_prefix} apply failed with {exception.__class__.__name__} exception: {exception}')
                return
        elif signal_delete.has_receivers_for(custom_resource):
            logger.debug(f'{logger_prefix} adding finalizer {FINALIZER_NAME}.')
            patch_object.setdefault('metadata', {})
            patch_object['metadata'].setdefault('finalizers', [])
            patch_object['metadata']['finalizers'].append(FINALIZER_NAME)

    logger.debug(f'{logger_prefix} patching resource object.')
    custom_objects_api_instance.patch_namespaced_custom_object(group, version, namespace, resource, name, patch_object)


def _custom_resource_events_consumer(custom_resource: CustomResource, uid: str, queue: Queue) -> None:
    while True:
        # Consume all but most recent change.
        # It is safe to use qsize() as we're the only consumer and qsize() can only increase in the mean time.
        for i in range(1, queue.qsize()):
            queue.get()
            queue.task_done()

        try:
            resource_object = queue.get(timeout=QUEUE_TIMEOUT)
        except Empty:
            logger.debug(f'Consumer for {uid} terminating, no work for {QUEUE_TIMEOUT} seconds.')
            break

        if resource_object is None:
            logger.debug(f'Consumer for {uid} terminating, got a poison pill.')
            break

        logger_prefix = _logger_prefix_from_resource_object(resource_object)
        logger.debug(f'{logger_prefix} calling handler.')
        try:
            _handle_custom_resource_change(custom_resource, resource_object)
        finally:
            logger.info(f'{logger_prefix} has been handled.')
            queue.task_done()


def _collect_consumers(resource_queues: Dict[str, Queue], resource_futures: Dict[Future, str],
                       timeout: int = None) -> None:
    futures = list(resource_futures.keys())
    try:
        for future in concurrent.futures.as_completed(futures, timeout=timeout):
            uid = resource_futures[future]
            del resource_queues[uid]
            del resource_futures[future]
            try:
                future.result()
            finally:
                logger.debug(f'Collected result for consumer {uid}.')
    except concurrent.futures.TimeoutError:
        pass


def _process_custom_resource_events(custom_resource: CustomResource) -> None:
    custom_objects_api_instance = kubernetes.client.CustomObjectsApi()

    group = custom_resource.group
    version = custom_resource.version
    resource = custom_resource.resource

    global_resource_version = 0
    resource_queues: Dict[str, Queue] = {}
    resource_futures: Dict[Future, str] = {}
    try:
        while True:
            if operator_shutdown.is_set():
                break

            watch = kubernetes.watch.Watch()
            event_generator = watch.stream(custom_objects_api_instance.list_cluster_custom_object,
                                           group,
                                           version,
                                           resource,
                                           resource_version=global_resource_version,
                                           _request_timeout=WATCH_TIMEOUT)

            try:
                logger.debug(f'Waiting for events related to {custom_resource}.')
                for event in event_generator:
                    if operator_shutdown.is_set():
                        break

                    global_resource_version = event['raw_object']['metadata']['resourceVersion']
                    logger.debug(f'Current resourceVersion is {global_resource_version}.')

                    resource_object = event['object']
                    logger_prefix = _logger_prefix_from_resource_object(resource_object)
                    event_type = event['type']
                    logger.debug(f'{logger_prefix} {event_type}.')

                    if event_type in ["ADDED", "MODIFIED"]:
                        uid = resource_object['metadata']['uid']

                        if uid not in resource_queues:
                            logger.debug(f'{logger_prefix} does not have a pre-existing queue, creating one.')

                            resource_queue: Queue = Queue()
                            resource_queues[uid] = resource_queue

                            logger.debug(f'{logger_prefix} queueing.')
                            resource_queues[uid].put(resource_object)

                            def custom_resource_events_consumer_wrapper(custom_resource=custom_resource,
                                                                        resource_queue=resource_queue,
                                                                        uid=uid):
                                _custom_resource_events_consumer(custom_resource, uid, resource_queue)

                            future = executor.submit(custom_resource_events_consumer_wrapper)
                            resource_futures[future] = uid
                        else:
                            logger.debug(f'{logger_prefix} queueing.')
                            resource_queues[uid].put(resource_object)
            except ReadTimeoutError:
                pass
            except ApiException as exception:
                if exception.status == 404:
                    logger.info(f'CRD {custom_resource} not found, retrying in {CRD_NOT_FOUND_RETRY_INTERVAL} seconds.')
                    time.sleep(CRD_NOT_FOUND_RETRY_INTERVAL)
                else:
                    raise
            finally:
                watch.stop()

            _collect_consumers(resource_queues, resource_futures, timeout=0)
    finally:
        for queue in resource_queues.values():
            queue.put(None)
        _collect_consumers(resource_queues, resource_futures)


def register_custom_resource(custom_resource: CustomResource) -> None:
    custom_resources.append(custom_resource)


def _handle_signal(signal, frame) -> None:
    logging.info(f'Received exit signal {signal}, shutting down, please wait.')
    operator_shutdown.set()


def run() -> NoReturn:
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        signal.signal(s, _handle_signal)

    futures: List[Future] = []
    for custom_resource in custom_resources:

        def process_custom_resource_events_wrapper(custom_resource=custom_resource):
            _process_custom_resource_events(custom_resource)

        futures.append(executor.submit(process_custom_resource_events_wrapper))

    for future in concurrent.futures.as_completed(futures):
        future.result()

    logger.debug('Shutting down thread pool executor.')
    executor.shutdown()
    sys.exit(0)
