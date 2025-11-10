def handler(params, context):
    print(f"Invoked inc with input: {params}")
    return int(params["n"]) + 1
