function handler(params, context) {
    console.log(params);
    console.log("" + params["n"]);
    return parseInt(params["n"], 10) + 1
}

module.exports = handler;
