import logging
from typing import Dict
from pprint import pprint

import k8s_mini_operator as operator

logger = logging.getLogger()

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(module)s:%(funcName)s:%(lineno)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

example_cr = operator.CustomResource(group='example.com', resource='examples', version='v1')


@operator.signal_apply.connect_via(example_cr)
def apply(sender: operator.CustomResource, resource_object: Dict):
    pprint({'action': 'APPLY', 'sender': sender, 'resoure_object': resource_object})
    return {'conditions': [{'type': 'Message', 'status': 'Job received'}, {'type': 'Ready', 'status': 'False'}]}


@operator.signal_delete.connect_via(example_cr)
def delete(sender: operator.CustomResource, resource_object: Dict):
    pprint({'action': 'DELETE', 'sender': sender, 'resoure_object': resource_object})


operator.register_custom_resource(example_cr)
operator.run()
