import asyncio
import functools
import json
import logging
import os
import subprocess

import aiojobs
import kubernetes
import ruamel.yaml

from .utils import parse

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger()


def logger_prefix_from_resource_object(resource_object):
    namespace = resource_object['metadata']['namespace']
    name = resource_object['metadata']['name']
    uid = resource_object['metadata']['uid']
    return "{}/{} ({})".format(namespace, name, uid)


async def handle_resource_change(apply_fn, delete_fn, api_update, api_delete, resource_object):
    logger_prefix = logger_prefix_from_resource_object(resource_object)

    resource_object['metadata'].setdefault('finalizers', [])
    patch_object = {}
    if resource_object['metadata'].get('deletionTimestamp', None) is not None:
        logger.info("{} marked for deletion".format(logger_prefix))
        if "Side8OperatorDelete" not in resource_object['metadata']['finalizers']:
            # We've already washed our hands of this one
            return

        logger.debug("{} calling delete_fn".format(logger_prefix))
        try:
            patch_object['status'] = await delete_fn(resource_object)
        except subprocess.CalledProcessError as e:
            # TODO generate K8s event
            logger.warning("{} {} exited with {}".format(logger_prefix, e.cmd, e.returncode))
            return
        except subprocess.TimeoutExpired as e:
            # TODO generate K8s event
            logger.warning('{} {} timed out after {} seconds'.format(logger_prefix, e.cmd, e.timeout))
            return

        if not patch_object['status']:
            logger.debug("{} delete_fn returned empty status, removing finalizer".format(logger_prefix))
            patch_object['metadata'] = {
                'finalizers': list(
                    filter(lambda f: f != "Side8OperatorDelete", resource_object['metadata']['finalizers']))
            }
    else:
        logger.info("{} triggered by change".format(logger_prefix))
        if "Side8OperatorDelete" in resource_object['metadata']['finalizers']:
            logger.debug("{} calling apply_fn".format(logger_prefix))
            try:
                patch_object['status'] = await apply_fn(resource_object)
            except subprocess.CalledProcessError as e:
                # TODO generate K8s event
                logger.warning("{} {} exited with {}".format(logger_prefix, e.cmd, e.returncode))
                return
            except subprocess.TimeoutExpired as e:
                # TODO generate K8s event
                logger.warning('{} {} timed out after {} seconds'.format(logger_prefix, e.cmd, e.timeout))
                return
        else:
            logger.debug("{} adding finalizer".format(logger_prefix))
            patch_object.setdefault('metadata', {})
            patch_object['metadata'].setdefault('finalizers', [])
            patch_object['metadata']['finalizers'].append("Side8OperatorDelete")

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
                api_delete = functools.partial(
                    custom_objects_api_instance.delete_namespaced_custom_object,
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

                # Closures, references, it's all so confusing.
                # Don't pass in the bits as default args like this and you'll have the del running on the wrong resources
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


async def generator_wrapper(sync_generator, _loop=None):
    _loop = asyncio.get_event_loop() if _loop is None else _loop
    while True:
        yield await _loop.run_in_executor(None, next, sync_generator)


async def api_events_sink(custom_objects_api_instance, group, version, resource, queue):
    w = kubernetes.watch.Watch()
    event_generator = w.stream(custom_objects_api_instance.list_cluster_custom_object, group, version, resource)
    async for event in generator_wrapper(event_generator):
        queue.put_nowait(event)


async def callout_fn(callback, timeout, resource_object):
    logger_prefix = logger_prefix_from_resource_object(resource_object)

    logger.debug("{} running {}".format(logger_prefix, callback))

    subprocess_env = dict([("_DOLLAR", "$")] + parse(resource_object, prefix="K8S") + [("K8S",
                                                                                        json.dumps(resource_object))])
    process = await asyncio.create_subprocess_exec(
        callback,
        env=dict(list(os.environ.items()) + list(subprocess_env.items())),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        out, err = await asyncio.wait_for(process.communicate(), timeout)
    except asyncio.TimeoutError:
        process.terminate()
        raise subprocess.TimeoutExpired(callback, timeout)

    out_decoded = out.decode('utf-8')
    err_decoded = err.decode('utf-8', errors='ignore')

    logger.debug("{} stdout: {}".format(logger_prefix, out_decoded))
    logger.debug("{} stderr: {}".format(logger_prefix, err_decoded))

    if process.returncode != 0:
        logger.error("{} stderr: {}".format(logger_prefix, err_decoded))
        raise subprocess.CalledProcessError(process.returncode, callback)

    status = ruamel.yaml.load(out_decoded, Loader=ruamel.yaml.SafeLoader)
    return status


def main():

    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, allow_abbrev=False)
    parser.add_argument('--group', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--resource', required=True)
    parser.add_argument('--apply', default="./apply")
    parser.add_argument('--delete', default="./delete")
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'ERROR'])

    args = parser.parse_args()

    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(module)s:%(funcName)s:%(lineno)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(args.log_level)

    try:
        kubernetes.config.load_incluster_config()
        logger.debug("configured in cluster with service account")
    except Exception:
        try:
            kubernetes.config.load_kube_config()
            logger.debug("configured via kubeconfig file")
        except Exception:
            raise RuntimeError('No Kubernetes configuration found.')

    custom_objects_api_instance = kubernetes.client.CustomObjectsApi()

    group = args.group
    version = args.version
    resource = args.resource

    apply_fn = functools.partial(callout_fn, args.apply, args.timeout)
    delete_fn = functools.partial(callout_fn, args.delete, args.timeout)

    loop = asyncio.get_event_loop()

    if args.log_level == "DEBUG":
        loop.set_debug(True)

    queue = asyncio.Queue()

    events_consumer_coro = events_consumer(custom_objects_api_instance, group, version, resource, apply_fn, delete_fn,
                                           queue)
    api_events_sink_coro = api_events_sink(custom_objects_api_instance, group, version, resource, queue)

    loop.run_until_complete(
        asyncio.wait({api_events_sink_coro, events_consumer_coro}, return_when=asyncio.FIRST_COMPLETED))


if __name__ == '__main__':
    main()
