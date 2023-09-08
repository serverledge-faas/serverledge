import hashlib


def handler(params, context):
    n = params["n"]
    return ''.join(hash_string(n))


# Hash string
def hash_string(s):
    return hashlib.sha256(s.encode()).hexdigest()
