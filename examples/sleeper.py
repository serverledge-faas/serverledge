import time


def handler(params, context):
    n = params["n"]
    sleeper(int(n))
    return ''


def sleeper(n):
    time.sleep(n)
