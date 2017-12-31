import gzip
import json
import os, re
import cStringIO
from twitter import *
from nltk.corpus import words
from nltk.corpus import stopwords

splitter = "c565f1-780a-4411-b2cb-d7153aa2ad60"
INDEX_DATA = {}
INDEX_COUNT = {}
DOC_ID_TO_FILE_NAME = {}

nltk_stopwords = list(stopwords.words('english'))
settings = {}
try:
    settings = json.loads(open('settings.json', 'r').read())
except:
    print('Error reading settings')
    exit(1)
TWEETS_PATH = '/Users/Yasir/PycharmProjects/nlp/tweets2'
oauth = OAuth(settings.get('ACCESS_TOKEN'), settings.get('ACCESS_SECRET'), settings.get('CONSUMER_KEY'),
              settings.get('CONSUMER_SECRET'))


def handle_twitter_stream(user_name):
    # Initiate the connection to Twitter Streaming API
    twitter = Twitter(auth=oauth)

    # Get a sample of the public data following through Twitter
    iterator = twitter.statuses.user_timeline(screen_name=user_name, count=500)
    i = 0
    last_id = ''
    tweet = {}
    tweets = []
    min_id = None
    while (i < 1200):
        for tweet in iterator:
            # Twitter Python Tool wraps the data returned by Twitter
            # as a TwitterDictResponse object.
            # We convert it back to the JSON format to print/score
            # print json.dumps(tweet)

            # The command below will do pretty printing for JSON data, try it out
            tweet_text = tweet.get('text')
            tweets.append(tweet_text)
            if not min_id:
                min_id = tweet.get('id')
            i += 1
        iterator = twitter.statuses.user_timeline(screen_name=user_name, count=500, max_id=tweet.get('id'))
    max_id = tweet.get('id')
    with open("{0}/{1}.ids".format(TWEETS_PATH, user_name), 'w+') as f:
        f.write('[{}, {}]'.format(min_id, max_id))
    with open("{0}/{1}".format(TWEETS_PATH, user_name), 'w+') as f:
        for line in tweets:
            if line.strip():
                f.write(u''.join((line)).encode('utf-8').strip() + '\n')


def find_common(docs_to_ret, param):
    i = 0
    j = 0
    res = []
    while i < len(docs_to_ret) and j < len(param):
        first = int(docs_to_ret[i])
        second = int(param[j])
        if first == second:
            res.append(first)
            i += 1
            j += 1
        elif first < second:
            i += 1
        else:
            j += 1
    return res


def normalize_word(word):
    word = re.sub('\W+', '', word).strip().lower()
    return '' if word in nltk_stopwords else word


def main(all_docs, search_terms):
    state_space = []
    doc_id = -1
    for doc_name in all_docs:
        doc_id += 1
        doc = all_docs[doc_name]
        DOC_ID_TO_FILE_NAME[doc_id] = doc_name
        for word in doc.split(" "):
            word = normalize_word(word)
            state_space.append("{0}{1}{2}".format(word, splitter, doc_id))
    state_space.sort()
    for key in state_space:
        res = key.split(splitter)
        str_key = res[0].strip()
        str_value = res[1].strip()
        if not INDEX_DATA.get(str_key):
            INDEX_DATA[str_key] = [str_value]
            INDEX_COUNT[str_key] = 1
        elif str_value not in INDEX_DATA[str_key]:
            INDEX_DATA[str_key].append(str_value)
            INDEX_COUNT[str_key] += 1


def print_doc_names(docs_to_ret, file_name_map):
    count = {}
    for c in docs_to_ret:
        count[c] = 1 if (c not in count) else count[c] + 1
    for i in count:
        if int(i) in file_name_map:
            print("Found Doc: {0}, id: {1} and count: {2}".format(file_name_map[int(i)], i,
                                                                  float(count[i]) / len(docs_to_ret)))
        elif str(i) in file_name_map:
            print("Found Doc: {0}, id: {1} and count: {2}".format(file_name_map[str(i)], i,
                                                                  float(count[i]) / len(docs_to_ret)))
        else:
            print("Some Error for doc: " + str(i))


def search_and_in_idx(tokens, index):
    docs_to_ret = []
    first_term = True
    for term in tokens:
        term = term.strip()
        if term in index:
            if not term:
                pass
            if first_term:
                docs_to_ret = index[term]
                first_term = False
            else:
                docs_to_ret = find_common(docs_to_ret, index[term])
    return docs_to_ret


def search_or_in_idx(tokens, index):
    docs_to_ret = []
    for term in tokens:
        term = normalize_word(term)
        if term in index:
            if not term:
                continue
            docs_to_ret += index[term]
            if term in words.words():
                # if a non dictionary word, weight it twice
                docs_to_ret += index[term] + index[term]

    return docs_to_ret


def decompress_string_from_file(filename='idx.dat'):
    """
    decompress the given string value (which must be valid compressed gzip
    data) and write the result in the given open file.
    """

    # stream = cStringIO.StringIO(open('idxfile', 'r').read())
    decompressor = gzip.GzipFile(filename=filename, mode='r')
    out_string = ''
    while True:  # until EOF
        chunk = decompressor.read(8192)
        if not chunk:
            decompressor.close()
            return out_string
        out_string += chunk


def compress_string_to_file(value, filename='idx.dat'):
    """
    read the given open file, compress the data and return it as string.
    """
    stream = cStringIO.StringIO(json.dumps(value))
    compressor = gzip.GzipFile(filename, mode='w')
    while True:  # until EOF
        chunk = stream.read(8192)
        if chunk:  # EOF?
            compressor.write(chunk)
        else:
            return

def populate_twitter_data():
    streams_to_handle = ["iamamirofficial", "iamAhmadshahzad"]
    for s in streams_to_handle:
        handle_twitter_stream(s)

def search_string():
    # search_term = 'We hosted a Town Hall in New Delhi with @BarackObama and young leaders'
    # search_term = "On Tuesday, SpaceX will attempt to refly both an orbital rocket and spacecraft for the first"
    # search_term = "What a way 2 serve ur country.. what a way 2 show endurance. A fantastic achievement brother... https://t.co/uvzLRcKLv3"
    search_term = "I wld specially Thank my Nation &amp; fans for supporting me throughout"
    index_file_name = 'indexes/idx_tweets.dat'
    index_exists = os.path.exists(index_file_name)
    if index_exists:
        file_data = decompress_string_from_file(index_file_name)
        data = json.loads(file_data)
        search_from_tokens(data, search_term)
    else:
        path = TWEETS_PATH
        docs_list = {}
        files_list = list(os.listdir(path))
        if not files_list:
            populate_twitter_data()
        for filename in os.listdir(path):
            if filename.endswith('.ids'):
                continue
            with open("{0}/{1}".format(path, filename)) as f:
                read_data = f.read()
                docs_list[filename] = read_data

        main(docs_list, search_term)
        idx_data = {
            'INDEX_DATA': INDEX_DATA,
            'DOC_ID_TO_FILE_NAME': DOC_ID_TO_FILE_NAME,
        }
        search_from_tokens(idx_data, search_term)
        compress_string_to_file(idx_data, index_file_name)


def search_from_tokens(data, search_term):
    # tokens = search_term.replace(" ", " AND ").split("AND")
    # search_data = search_and_in_idx(tokens, data.get('INDEX_DATA'))
    tokens = search_term.replace(" ", " OR ").split("OR")
    search_data = search_or_in_idx(tokens, data.get('INDEX_DATA'))
    print_doc_names(search_data, data.get('DOC_ID_TO_FILE_NAME'))


if __name__ == '__main__':
    # search_string()
    # populate_twitter_data()
    search_string()
