import importlib


def get_attr(method):
    method_split = method.split('.')
    module_name = '.'.join(method_split[0:-1])
    attr_name = method_split[-1]
    module = importlib.import_module(module_name)
    attr = getattr(module, attr_name)
    return attr


def transform():
    def wrapper(fn):
        def run(inputs, output, arguments={}):
            inputs = [get_attr(input_table) for input_table in inputs]
            output = get_attr(output)

            df = fn(inputs=inputs, output=output, arguments=arguments)

            return df

        return run

    return wrapper
