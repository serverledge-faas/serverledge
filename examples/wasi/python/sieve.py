import sys
import json


def sieve(limit: int) -> list[int]:
    sieve: list[bool] = [False] * (limit + 1)
    primes: list[int] = list()
    for i in range(2, limit + 1):
        if not sieve[i]:
            primes.append(i)
            for j in range(i << 1, limit + 1, i):
                sieve[j] = True
    return primes


if __name__ == "__main__":
    limit = 100000
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
        if "limit" in params:
            limit = int(params["limit"])
    primes = sieve(limit)

    print(primes)


def handler(params, context):
    limit = 100000
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
        if "limit" in params:
            limit = int(params["limit"])
    primes = sieve(limit)

    print(primes)
    return primes
