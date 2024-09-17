
from typing import List, Dict
from copy import deepcopy

from ..context.context import Context
from .node import Node


def resolve_context_input(input_name: str, context: Context):
    '''
    Deduce input value and handle context syntax
    '''

    if input_name == 'context':
        return context
    
    elif input_name.startswith('context.catalog'):
        _, _, key = input_name.split('.')
        return context.catalog(key)

    elif input_name.startswith('context.'):
        _, attr, keys = input_name.split('.')

        output = getattr(context, attr)[keys[0]]
        for k in keys[1:]:
            output = output[k]
        return output
    
    else:
        return input_name


def map_input_to_value(node_inputs, results: Dict, context: Context):
    '''
    Map node input key to value using context and output of previous nodes (results dictionary)
    '''
    
    if isinstance(node_inputs, Dict):
        resolved_node_inputs = {
            key: results.get(resolve_context_input(item, context), resolve_context_input(item, context)) for key, item in node_inputs.items()
        }
    elif isinstance(node_inputs, List):
        resolved_node_inputs = [
            results.get(resolve_context_input(inp, context), resolve_context_input(inp, context)) for inp in node_inputs
        ]
    else:
        resolved_input = resolve_context_input(node_inputs, context)
        resolved_node_inputs = results.get(resolved_input, resolved_input)

    return resolved_node_inputs


def store_outputs_in_results(outputs_key, outputs_value, results: Dict):
    '''
    Save output in results map to be used by next nodes
    '''
    results_copy = deepcopy(results)

    if isinstance(outputs_key, List):
        for output_name, output_value in zip(outputs_key, outputs_value):
            results_copy[output_name] = output_value
    else:
        results_copy[outputs_key] = outputs_value
    return results_copy


class Pipeline:
    '''
    Execute list of nodes
    '''
    def __init__(self, nodes: List[Node]) -> None:
        self.nodes=nodes

    def run(self, context: Context):
        
        results = {}
        for node in self.nodes:

            resolved_node_inputs = map_input_to_value(node_inputs=node.inputs, results=results, context=context)
            node_output_value = node.run(resolved_node_inputs)
            results = store_outputs_in_results(outputs_key=node.outputs, outputs_value=node_output_value, results=results)
        return results

