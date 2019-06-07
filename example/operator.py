import asyncio
import logging
import os
from typing import Dict
from pprint import pprint

import elemental.k8s.operator as operator

logger = logging.getLogger()

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(module)s:%(funcName)s:%(lineno)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


async def apply(resoure_object: Dict):
    pprint({'action': 'APPLY', 'resoure_object': resoure_object})
    return {'conditions': [{'type': 'Message', 'status': 'Job received'}, {'type': 'Ready', 'status': 'False'}]}


async def delete(resource_object: Dict):
    pprint({'action': 'DELETE', 'resoure_object': resource_object})


operator.register_custom_resource('example.com', 'v1', 'examples', apply, delete)
operator.run()
