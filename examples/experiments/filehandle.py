import os

def handler(params, context):
    n = int(params.get("n", 1000000))
    file_path = "/tmp/dummy_handle.txt"

    with open(file_path, "w") as f:
        f.write("test data")

    for _ in range(n):
        f = open(file_path, "r")
        f.close()

    os.remove(file_path)
    return {"message": f"Eseguiti {n} cicli di file handle", "n": n}