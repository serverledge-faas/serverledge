import time


def handler(params, context):
    n = params["n"]
    return ''.join(sleeper(int(n)))


def sleeper(n):
    time.sleep(n)

    return "awake"