// Example ported from TinyFaaS (https://github.com/OpenFogStack/tinyFaaS/blob/master/examples/sieve-of-erasthostenes/index.js)
//
module.exports = (params, ctx) => {
	var max;

	if (params["n"] == undefined) {
		max = 1000
	} else {
		max = parseInt(params["n"],10);
	}

	let sieve = [], i, j, primes = [];
	for (i = 2; i <= max; ++i) {

		if (!sieve[i]) {
			primes.push(i);
			for (j = i << 1; j <= max; j += i) {
				sieve[j] = true;
			}
		}
	}

	result = {"N": max, "Primes": primes}
	return result
}
