import numpy as np
import os
import time

def handler(params, context):

    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"

    n = int(params.get("n", 4000))

    A = np.random.rand(n, n)
    B = np.random.rand(n)

    start_time = time.time()
    x = np.linalg.solve(A, B)
    end_time = time.time()

    execution_time = end_time - start_time

    return {
        "status": "success",
        "matrix_size": n,
        "calc_time_seconds": round(execution_time, 4)
    }