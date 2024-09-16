
from dataclasses import dataclass
from typing import Union, Dict
import yaml
import os

@dataclass
class DataCatalog:
    '''
    Simple class to easily manage dataset using parameters stored in yaml file.
    You can either pass the path to the yaml file (as a string) or directly a dictionary
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
        return
    

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

        if data_conf['filepath'].startswith('@abs:'):
            full_filepath = data_conf['filepath'].replace('@abs:', '')
        else: 
            full_filepath = os.path.join(self.source_path, data_conf['filepath'])
        


        if data_conf['type'] == 'pandas.csv':
            import pandas as pd
            return pd.read_csv(full_filepath, **load_kwargs)
        elif data_conf['type'] == 'pandas.parquet':
            import pandas as pd
            return pd.read_parquet(full_filepath, **load_kwargs)
        elif data_conf['type'] == 'pandas.excel':
            import pandas as pd
            return pd.read_excel(full_filepath, **load_kwargs)
        elif data_conf['type'] == 'pandas.hdf':
            import pandas as pd
            return pd.read_hdf(full_filepath, **load_kwargs)
        elif data_conf['type'] == 'pandas.pickle':
            import pandas as pd
            return pd.read_pickle(full_filepath, **load_kwargs)
        elif data_conf['type'] in ('yaml', 'yml'):
            return yaml.safe_load(open(full_filepath))
        elif data_conf['type'] == 'polars.csv':
            import polars as pl
            return pl.read_csv(full_filepath, **(load_kwargs or {}))
        elif data_conf['type'] == 'polars.parquet':
            import polars as pl
            return pl.read_parquet(full_filepath, **(load_kwargs or {}))
        else:
            raise ValueError(f"Unsupported data type: {data_conf['type']}")


    def __call__(self, key: str):
        if key not in self.catalog_parameters:
            raise ValueError(f'Unkown key {key} in datacatalg') 
        data_conf = self.catalog_parameters[key]
        return self._load_data(data_conf=data_conf)