import asyncio
import functools
import logging
import signal
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import suppress
from typing import Awaitable, List, Optional

import aiojobs
import kubernetes
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError

FINALIZER_NAME = 'PyKubeOperatorDelete'

logger = logging.getLogger()

handler_coros: List[Awaitable] = []

try:
    kubernetes.config.load_incluster_config()
    logger.debug('Configured in cluster with service account.')
except Exception:
    try:
        kubernetes.config.load_kube_config()
        logger.debug('Configured via kubeconfig file.')
    except Exception:
        raise RuntimeError('No Kubernetes configuration found.')

custom_objects_api_instance = kubernetes.client.CustomObjectsApi()

loop = asyncio.get_event_loop()


def logger_prefix_from_resource_object(resource_object):
    namespace = resource_object['metadata']['namespace']
    name = resource_object['metadata']['name']
    uid = resource_object['metadata']['uid']
    return "{}/{} ({})".format(namespace, name, uid)


async def handle_resource_change(apply_fn, delete_fn, api_update, api_delete, resource_object):
    logger_prefix = logger_prefix_from_resource_object(resource_object)

    resource_object['metadata'].setdefault('finalizers', [])
    patch_object = {}
    if delete_fn is not None and resource_object['metadata'].get('deletionTimestamp', None) is not None:
        logger.info("{} marked for deletion".format(logger_prefix))

        if FINALIZER_NAME not in resource_object['metadata']['finalizers']:
            # We've already washed our hands of this one
            return

        logger.debug("{} calling delete_fn".format(logger_prefix))
        patch_object['status'] = await delete_fn(resource_object)

        if not patch_object['status']:
            logger.debug("{} delete_fn returned empty status, removing finalizer".format(logger_prefix))
            patch_object['metadata'] = {
                'finalizers': list(filter(lambda f: f != FINALIZER_NAME, resource_object['metadata']['finalizers']))
            }
    else:
        logger.info("{} triggered by change".format(logger_prefix))

        if delete_fn is None or FINALIZER_NAME in resource_object['metadata']['finalizers']:
            logger.debug("{} calling apply_fn".format(logger_prefix))
            patch_object['status'] = await apply_fn(resource_object)
        elif delete_fn is not None:
            logger.debug("{} adding finalizer".format(logger_prefix))
            patch_object.setdefault('metadata', {})
            patch_object['metadata'].setdefault('finalizers', [])
            patch_object['metadata']['finalizers'].append(FINALIZER_NAME)

    logger.debug("{} calling api update".format(logger_prefix))
    api_update(patch_object)


async def resource_events_consumer(apply_fn, delete_fn, api_update, api_delete, queue, logger_prefix):
    while True:
        if not queue.qsize():
            logger.debug("{} falling out of empty consumer".format(logger_prefix))
            return
        # consume all but most recent change
        for i in range(1, queue.qsize()):
            queue.get_nowait()
            queue.task_done()
        resource_object = queue.get_nowait()

        namespace = resource_object['metadata']['namespace']
        name = resource_object['metadata']['name']
        uid = resource_object['metadata']['uid']

        logger_prefix = "{}/{} ({})".format(namespace, name, uid)
        logger.debug("{} event".format(logger_prefix))

        logger.debug("{} scheduling handler".format(logger_prefix))
        try:
            await handle_resource_change(apply_fn, delete_fn, api_update, api_delete, resource_object)
        finally:
            logger.info("{} has been handled".format(logger_prefix))
            queue.task_done()


async def events_consumer(custom_objects_api_instance, group, version, resource, apply_fn, delete_fn, queue):
    scheduler = await aiojobs.create_scheduler(limit=10)

    resource_queues = {}
    while True:
        event = await queue.get()

        resource_object = event['object']
        logger_prefix = logger_prefix_from_resource_object(resource_object)
        event_type = event['type']
        logger.debug("{} {}".format(logger_prefix, event_type))

        if event_type in ["ADDED", "MODIFIED"]:
            namespace = resource_object['metadata']['namespace']
            name = resource_object['metadata']['name']
            uid = resource_object['metadata']['uid']

            if uid not in resource_queues:
                logger.debug("{} does not have a pre-existing consumer, spawning one".format(logger_prefix))
                api_update = functools.partial(custom_objects_api_instance.patch_namespaced_custom_object, group,
                                               version, namespace, resource, name)
                api_delete = functools.partial(custom_objects_api_instance.delete_namespaced_custom_object,
                                               group,
                                               version,
                                               namespace,
                                               resource,
                                               name,
                                               body=kubernetes.client.V1DeleteOptions())
                resource_queue = asyncio.Queue()
                logger.debug("{} queueing".format(logger_prefix))
                resource_queue.put_nowait(resource_object)
                resource_queues[uid] = resource_queue

                # Closure in Python are late binding. If we don't pass in the bits as default args like this we'll
                # have the delete running on the wrong resources.
                async def resource_events_consumer_wrapper(uid=uid,
                                                           apply_fn=apply_fn,
                                                           delete_fn=delete_fn,
                                                           api_update=api_update,
                                                           api_delete=api_delete,
                                                           resource_queue=resource_queue,
                                                           logger_prefix=logger_prefix):
                    try:
                        await resource_events_consumer(apply_fn, delete_fn, api_update, api_delete, resource_queue,
                                                       logger_prefix)
                    finally:
                        logger.debug("{} has completed, removing GC preventing reference".format(logger_prefix))
                        del (resource_queues[uid])

                await scheduler.spawn(resource_events_consumer_wrapper())
            else:
                logger.debug("{} queueing".format(logger_prefix))
                resource_queues[uid].put_nowait(resource_object)

        queue.task_done()


async def _async_generator_wrapper(sync_generator):
    while True:
        yield await loop.run_in_executor(None, next, sync_generator)


async def api_events_sink(custom_objects_api_instance, group, version, resource, queue):
    resource_version = 0
    while True:
        watch = kubernetes.watch.Watch()
        event_generator_sync = watch.stream(custom_objects_api_instance.list_cluster_custom_object,
                                            group,
                                            version,
                                            resource,
                                            resource_version=resource_version,
                                            _request_timeout=10)
        event_generator = _async_generator_wrapper(event_generator_sync)

        try:
            async for event in event_generator:
                resource_version = event['raw_object']['metadata']['resourceVersion']
                logger.debug('Current resourceVersion is {}.'.format(resource_version))
                queue.put_nowait(event)
        except ReadTimeoutError:
            pass
        except ApiException as exception:
            if exception.status == 404:
                logger.info('CRD {}.{}/{} not found, retrying in 5 seconds.'.format(resource, group, version))
                await asyncio.sleep(5)
            else:
                raise
        finally:
            watch.stop()


async def handle_exception(fn):
    try:
        await fn
    except asyncio.CancelledError:
        logging.info('Coroutine canceled.')
    except Exception as exception:
        logging.error('Corouting raised {} exception: {}.'.format(exception.__class__.__name__, exception))
    finally:
        loop.stop()


def register_custom_resource(group: str,
                             version: str,
                             resource: str,
                             apply: Awaitable,
                             delete: Optional[Awaitable] = None) -> None:
    queue = asyncio.Queue()
    events_consumer_coro = handle_exception(
        events_consumer(custom_objects_api_instance, group, version, resource, apply, delete, queue))
    api_events_sink_coro = handle_exception(
        api_events_sink(custom_objects_api_instance, group, version, resource, queue))

    handler_coros.append(events_consumer_coro)
    handler_coros.append(api_events_sink_coro)


def shutdown(signal) -> None:
    logging.info('Received exit signal {}, canceling all tasks.'.format(signal))

    for task in asyncio.all_tasks():
        task.cancel()


def run() -> None:
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: shutdown(s))

    if logger.getEffectiveLevel() == logging.DEBUG:
        loop.set_debug(True)

    try:
        for coro in handler_coros:
            loop.create_task(coro)
        loop.run_forever()
    finally:
        loop.close()
