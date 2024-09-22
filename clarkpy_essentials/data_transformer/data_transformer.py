
from typing import List, Dict
import inspect
from copy import deepcopy

from ..decorator.decorator import deepcopy_args

def check_function(f):
    signature = inspect.signature(f)
    params = signature.parameters

    for param in params.values():
        if param.name == 'X':
            return True
    return False


def check_instructions(instructions: List):
    for i in instructions:
        assert isinstance(i, Dict), 'Instructions must be a list of  dictionaries'
        assert 'type' in i, 'Every step must contain "type" key'


class DataTransformer():

    def __init__(self) -> None:
        self._transformer_catalog = {}
        pass

    
    @property
    def transformer_catalog(self): 
        return self._transformer_catalog


    def add_transformer(self, key: str, func):
        self._transformer_catalog[key] = func
    

    def add_transformer_catalog(self, catalog: Dict):
        self.transformer_catalog.update(catalog)
    

    def transform(self, X, instructions: List):
        check_instructions(instructions=instructions)

        for step in instructions:
            X = self._execute_step(X=X, step=step)
        return X
    

    def _unpack_step_info(self, step: Dict):
        type = step.get('type')
        step_kwargs = step.get('kwargs', {})
        step_func = self._transformer_catalog[type]
        return step_func, step_kwargs 

    @deepcopy_args
    def _execute_step(self, X, step: Dict):
        step_func, step_kwargs = self._unpack_step_info(step=step)
        X = step_func(X=X, **step_kwargs)
        return X