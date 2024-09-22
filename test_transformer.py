

from clarkpy_essentials.data_transformer.transformer import DataTransformer

if __name__ == '__main__':


    def f1(X):
        return X
    
    def f2(X, alpha):
        return X*alpha
    
    dt = DataTransformer()
    dt.add_transformer_catalog({
        'identity': f1,
        'mul': f2
    })



    instructions = [
        {
            'type': 'identity'
        },

        {
            'type': 'mul',
            'kwargs': {
                'alpha': 2
            }
        }
    ]


    X = [1,2,3,4,5]
    X_transformed = dt.transform(X=X, instructions=instructions)
    X_transformed