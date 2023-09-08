import re


def handler(params, context):
    return grep("grep", params["InputText"])


def grep(pattern, text):
    lines = text.split('\n')
    result = [line for line in lines if re.search(pattern, line)]
    return '\n'.join(result)
