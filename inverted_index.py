import gzip
import json
import os
import cStringIO
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)


def handle_twitter_stream():
    # Initiate the connection to Twitter Streaming API
    twitter_stream = TwitterStream(auth=oauth)

    # Get a sample of the public data following through Twitter
    iterator = twitter_stream.statuses.sample()
    i = 0
    for tweet in iterator:
        # Twitter Python Tool wraps the data returned by Twitter
        # as a TwitterDictResponse object.
        # We convert it back to the JSON format to print/score
        # print json.dumps(tweet)

        # The command below will do pretty printing for JSON data, try it out
        print json.dumps(tweet, indent=4)

        i+=1
        if i > 10:
            break


splitter = "c565f1-780a-4411-b2cb-d7153aa2ad60"
INDEX_DATA = {}
INDEX_COUNT = {}
DOC_ID_TO_FILE_NAME = {}


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


def main(all_docs, search_terms):
    state_space = []
    doc_id = -1
    for doc_name in all_docs:
        doc_id += 1
        doc = all_docs[doc_name]
        DOC_ID_TO_FILE_NAME[doc_id] = doc_name
        for word in doc.split(" "):
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
    tokens = search_terms.replace(" ", " AND ").split("AND")
    docs_to_ret = search_in_idx(tokens, INDEX_DATA)
    print_doc_names(docs_to_ret, DOC_ID_TO_FILE_NAME)


def print_doc_names(docs_to_ret, file_name_map):
    for i in docs_to_ret:
        if int(i) in file_name_map:
            print("Found Doc: {0}, id: {1}".format(file_name_map[int(i)], i))
        elif str(i) in file_name_map:
            print("Found Doc: {0}, id: {1}".format(file_name_map[str(i)], i))
        else:
            print("Some Error for doc: " + str(i))


def search_in_idx(tokens, index):
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
    stream = cStringIO.StringIO(value)
    compressor = gzip.GzipFile(filename, mode='w')
    while True:  # until EOF
        chunk = stream.read(8192)
        if chunk:  # EOF?
            compressor.write(chunk)
        else:
            return


if __name__ == '__main__':
    # handle_twitter_stream()
    # raise Exception('Break')
    search_term = 'awesome movie'
    index_file_name = 'idx.dat'
    index_exists = os.path.exists(index_file_name)
    if index_exists:
        file_data = decompress_string_from_file(index_file_name)
        data = json.loads(file_data)
        tokens = search_term.replace(" ", " AND ").split("AND")
        search_data = search_in_idx(tokens, data.get('INDEX_DATA'))
        print_doc_names(search_data, data.get('DOC_ID_TO_FILE_NAME'))
    else:
        path = '/Users/Yasir/Downloads/review_polarity.tar/txt_sentoken/pos'
        docs_list = {}
        for filename in os.listdir(path):
            with open("{0}/{1}".format(path, filename)) as f:
                read_data = f.read()
                docs_list[filename] = read_data

        main(docs_list, search_term)
        idx_data = {
            'INDEX_DATA': INDEX_DATA,
            'DOC_ID_TO_FILE_NAME': DOC_ID_TO_FILE_NAME,
        }
        compress_string_to_file(idx_data, index_file_name)


