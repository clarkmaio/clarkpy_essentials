
from typing import List, Union, Dict

class Node:
    def __init__(self, 
                 func, 
                 inputs: Union[List[str], str, Dict], 
                 outputs: Union[List[str], str, Dict],
                 name: str = None):
        self.func = func
        self.inputs = inputs
        self.outputs = outputs
        self._name = name

    @property
    def name(self):
        return self._name

    def run(self, inputs):
        
        if isinstance(self.inputs, List):
            return self.func(*inputs)
        
        elif isinstance(self.inputs, Dict):
            return self.func(**inputs)

        else:
            return self.func(inputs)