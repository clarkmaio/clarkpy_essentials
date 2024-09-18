

from clarkpy_essentials.context.context import Context
from clarkpy_essentials.flow.pipeline import Pipeline
from clarkpy_essentials.flow.node import Node


if __name__ == "__main__":
    
    def sum(x, y):
        z = x+y
        print('Sum', z)
        return z
    
    def prod(x, y):
        z = x*y
        print('Prod', z)
        return z

    def minus_1(x):
        return x-1, 1
    

    def noinput():
        return 1
    
    def nooutput():
        x=1

    def arrayfunc(v, const):
        return v*const
    
    context = Context(parser = {'var1': {'aa': 1, 'bb': 1},
                                'var2': 2},
                        array = [1,2,3,4,5,6])

    pipeline = Pipeline([
        

        
        Node(func=arrayfunc,
             inputs=['context.array', 2],
             outputs='new_array'),

        Node(func=noinput,
             inputs=None,
             outputs='foo_output'),

        Node(func=nooutput,
             inputs=None,
             outputs=None),

        Node(func=sum, 
             inputs=['context.parser.var1.aa', 'context.parser.var2'], 
             outputs='c'),

        Node(func=prod, 
             inputs=['context.parser.var1.bb', 'c'],
             outputs='d'),

        Node(func=minus_1, 
             inputs={'x': 'd'},
             outputs=['e', 'one']),
    ])

    results = pipeline.run(context=context)