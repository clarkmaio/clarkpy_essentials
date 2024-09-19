
from typing import Dict


def PandasLoader(type: str, filepath: str, load_kwargs: Dict = None):
    import pandas as pd

    load_kwargs = load_kwargs or {}
    
    if type.endswith('.csv'):
        return pd.read_csv(filepath, **load_kwargs)
    elif type.endswith('.excel'):
        return pd.read_excel(filepath, **load_kwargs)
    elif type.endswith('.parquet'):
        return pd.read_parquet(filepath, **load_kwargs)
    elif type.endswith('.hdf'):
        return pd.read_hdf(filepath, **load_kwargs)
    else:
        raise ValueError(f"Unsupported file type: {type}")

def PolarsLoader(type: str, filepath: str, load_kwargs: Dict = None):
    import polars as pl

    load_kwargs = load_kwargs or {}

    if type.endswith('.csv'):
        return pl.read_csv(filepath, **load_kwargs)
    elif type.endswith('.parquet'):
        return pl.read_parquet(filepath, **load_kwargs)
    elif type.endswith('.ipc'):
        return pl.read_ipc(filepath, **load_kwargs)
    elif type.endswith('.excel'):
        return pl.read_excel(filepath, **load_kwargs)
    else:
        raise ValueError(f"Unsupported file type: {type}")
    

def YamlLoader(type: str, filepath: str, load_kwargs: Dict = None):
    import yaml
    load_kwargs = load_kwargs or {}
        
    with open(filepath, 'r') as file:
        return yaml.safe_load(file, **load_kwargs)


def TorchLoader(type: str, filepath: str, load_kwargs: Dict = None):
    import torch
    load_kwargs = load_kwargs or {}
    return torch.load(filepath, **load_kwargs)


def JsonLoader(type: str, filepath: str, load_kwargs: Dict = None): 
    import json

    load_kwargs = load_kwargs or {}

    with open(filepath, 'r') as file:
        return json.load(file, **load_kwargs)


def PickleLoader(type: str, filepath, load_kwargs: Dict = None):
    import pickle

    load_kwargs = load_kwargs or {}
    with open(filepath, 'rb') as file:
        return pickle.load(file, **load_kwargs) 

def SqlLoader(type: str, query: str, load_kwargs: Dict = None):
    raise NotImplementedError('SqlLoader not implemented')