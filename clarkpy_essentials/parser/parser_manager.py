


from argparse import ArgumentParser
from typing import Dict, Union
import yaml



def parse_type(type: str):
    if type == 'str':
        return str
    elif type == 'int':
        return int
    elif type == 'float':
        return float
    elif type == 'bool':
        return bool
    else:
        return type

def parse_arg_params(item: Dict):
    if 'type' in item:
        item['type'] = parse_type(item['type'])
    return item


class ParserManager:

    def __init__(self):
        return

    @staticmethod    
    def load(path: str) -> Dict:
        '''
        Read parser instruction and parse into a dictionary.
        Also build argument parser to read params from cli.
        '''
        raw_parser_instructions = ParserManager.load_raw_instructions(path=path)
        parser_args = ParserManager.build_parser(parser_instructions=raw_parser_instructions)
        return parser_args
    
    @staticmethod
    def build_parser(parser_instructions: Dict = None) -> Dict:
        '''
        Create argument parser using raw yaml instructions
        '''

        parser = ArgumentParser()
        for key, item in parser_instructions.items():
            item_parsed = parse_arg_params(item=item)
            parser.add_argument(f'--{key}', **item_parsed)

        parser_args = vars(parser.parse_args())
        return parser_args
    
    @staticmethod
    def load_raw_instructions(path: str) -> Dict:
        return yaml.safe_load(open(path, 'r'))
        

if __name__ == '__main__':
    parser_instructions = {
        'var': {
            'default': '1',
            'type': 'float'
        }
    }
    args = ParserManager.build_parser(parser_instructions)