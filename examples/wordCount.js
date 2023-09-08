function handler(params, context) {
    return wordCount(params["InputText"])
}

function wordCount(text) {
    return text
        .trim()
        .split(/\s+/) // Split the text into words
        .map(word => word.trim()) // Remove leading and trailing spaces from each word
        .filter(word => word !== '') // Filter out empty words
        .reduce((count, word) => count + 1, 0); // Reduce to count the words
}

module.exports = handler;