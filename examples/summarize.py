import re
from collections import Counter


def handler(params, context):
    return summarize_text(params["InputText"])


def summarize_text(text, num_sentences=2):
    # Split the text into sentences
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)

    # Create a list of common keywords that may indicate sentence importance
    keywords = ['important', 'summary', 'key', 'highlight', 'main']

    # Calculate a score for each sentence based on sentence length and keyword frequency
    sentence_scores = {}
    for i, sentence in enumerate(sentences):
        sentence = sentence.lower()
        sentence_length = len(sentence.split())
        keyword_count = sum(sentence.count(keyword) for keyword in keywords)
        sentence_scores[i] = sentence_length + keyword_count

    # Get the top 'num_sentences' sentences with the highest scores
    top_sentences = sorted(sentence_scores, key=sentence_scores.get, reverse=True)[:num_sentences]

    # Generate the summary by concatenating the top sentences
    summary = " ".join(sentences[i] for i in top_sentences)

    return summary
