

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
    
    context = Context(parser = {'a': 1, 'b': 2})

    pipeline = Pipeline([
        Node(func=sum, 
             inputs=['context.parser.a', 'context.parser.b'], 
             outputs='c'),

        Node(func=prod, 
             inputs=['context.parser.a', 'c'],
             outputs='d'),

        Node(func=minus_1, 
             inputs={'x': 'd'},
             outputs=['e', 'one'])
    ])

    results = pipeline.run(context=context)