

import os
import pandas as pd

from clarkpy_essentials.context.context import Context
from clarkpy_essentials.flow.pipeline import Pipeline
from clarkpy_essentials.flow.node import Node
from clarkpy_essentials.data_catalog.data_catalog import DataCatalog

if __name__ == "__main__":
    
    # Node functions
    def f1(x: float, y: float) -> float:
        return x*y


    def f2(df: pd.DataFrame, z: float) -> pd.DataFrame:
        new_df = (df*z).T
        return new_df
    

    # ------------ Create Context --------------
    GLOBAL_VARIABLES = {'var1': 1, 'var2': 100, 'dataframe': pd.DataFrame(np.random.randn(10, 5))}
    context = Context(global_variables=GLOBAL_VARIABLES)

    # ------------ Initialize Piepline ---------
    pipeline = Pipeline([
        Node(func=f1,
            inputs=['context.global_variables.var1', 'context.global_variables.var2'],
            outputs='outpout_f1'),
        # Node(func=f2,
        #      inputs=['context.catalog.csv_test', 'outpout_f1'],
        #      outputs='outpout_f2')
    ])

    # ------------ Run Piepline ----------------
    pipeline_results = pipeline.run(context=context)