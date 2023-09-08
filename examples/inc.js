function handler(params, context) {
    console.log(params);
    console.log("" + params["input"]);
    return parseInt(params["input"], 10) + 1
}

module.exports = handler;