
from dataclasses import dataclass
from typing import Union, Dict
import yaml
import os

from .loader import PandasLoader, PickleLoader, PolarsLoader, YamlLoader, JsonLoader, TorchLoader, SqlLoader

@dataclass
class DataCatalog:
    '''
    Simple class to easily manage dataset using parameters stored in yaml file.
    You can either pass the path to the yaml file (as a string) or directly a dictionary

    :params catalog: can be either a string pointing to yaml file or a dictionary
    :params source_path: the source path that will be used to build fullpath of dataset. 
                         `fullpath` is obtained from the concatenation of `source_path` and `filepath`
    '''
    catalog: Union[str, Dict]
    source_path: str



    def __post_init__(self):

        if isinstance(self.catalog, Dict):
            self.catalog_parameters = self.catalog
        elif isinstance(self.catalog, str):
            self.catalog_parameters = yaml.safe_load(open(self.catalog))
        else:
            raise ValueError('catalog should be either a string or a dictionary')

        self.custom_loaders = {}

    
    def add_custom_loader(self, key: str, loader):
        self.custom_loaders[key] = loader 
    

    def _build_full_filepath(self, filepath: str) -> str:

        if filepath.startswith('@abs:'):
            full_filepath = filepath.replace('@abs:', '')
        else: 
            full_filepath = os.path.join(self.source_path, filepath)
        return full_filepath

    def _load_data(self, data_conf: Dict):
        '''
        Load data using parameters passed
        Standard parameters are:
        - type
        - filepath
        - load_kwargs
        '''

        assert 'type' in data_conf
        assert 'filepath' in data_conf

        load_kwargs =  {}
        if 'load_kwargs' in data_conf:
            load_kwargs = data_conf['load_kwargs']

        full_filepath = self._build_full_filepath(filepath=data_conf['filepath'])
        return self._load_data_core(type=data_conf['type'], full_filepath=full_filepath, load_kwargs=load_kwargs)


    def _load_data_core(self, type: str, full_filepath: str, load_kwargs):
        '''
        Load dataset depeding on the type.
        Each type will be loaded the correspoding Loader function.

        Check first of the type match the custom loader key
        '''

        # Custom Loaders
        if type in self.custom_loaders:
            raise NotImplementedError('Catalog can not yet handle custom loaders')


        # Builtin Loaders
        elif type.startswith('pandas.'):
            return PandasLoader(type=type, filepath=full_filepath, load_kwargs=load_kwargs)
        
        elif type.startswith('polars.'):
            return PolarsLoader(type=type, filepath=full_filepath, load_kwargs=load_kwargs)
        
        elif type in ('yaml', 'yml'):
            return YamlLoader(type=type, filepath=full_filepath, load_kwargs=load_kwargs)
        
        elif type == 'json':
            return JsonLoader(type=type, filepath=full_filepath, load_kwargs=load_kwargs)

        elif type == 'torch':
            return TorchLoader(type=type, filepath=full_filepath, load_kwargs=load_kwargs)

        elif type == 'pickle':
            return PickleLoader(type=type, filepath=full_filepath, load_kwargs=load_kwargs)

        else:
            raise ValueError(f"Unsupported data type: {type}")

    def __call__(self, key: str):
        if key not in self.catalog_parameters:
            raise ValueError(f'Unkown key {key} in catalog') 
        data_conf = self.catalog_parameters[key]
        return self._load_data(data_conf=data_conf)