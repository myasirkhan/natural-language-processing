import re

MAX_WORD_LEN = 3
MAX_PASS_ITERATION = 5
CURR_ITER = 0
NEUTRAL = ['youve', 'theres', 'sent', 'sometimes', 'cant', 'this', 'others', 'about']


def my_lexicon(p_lex, n_lex, text, but_indices, and_indices):
    local_pos = list(p_lex)
    local_neg = list(n_lex)
    all_txt = text
    found_match = False
    for index in but_indices:
        if text[index] != 'but':
            print 'Exception'
            break
        if all_txt[index - 1].lower() in local_pos and \
                        all_txt[index + 1].lower() not in local_neg and \
                        all_txt[index + 1].lower() not in local_pos and \
                        all_txt[index + 1].lower() not in NEUTRAL and \
                        all_txt[index - 1].lower() not in NEUTRAL and \
                        len(all_txt[index + 1]) > MAX_WORD_LEN:
            found_match = True
            local_neg.append(all_txt[index + 1].lower())
        if all_txt[index - 1].lower() in local_neg and \
                        all_txt[index + 1].lower() not in local_pos and \
                        all_txt[index + 1].lower() not in local_neg and \
                        all_txt[index + 1].lower() not in NEUTRAL and \
                        all_txt[index - 1].lower() not in NEUTRAL and \
                        len(all_txt[index + 1]) > MAX_WORD_LEN:
            found_match = True
            local_pos.append(all_txt[index + 1].lower())

    for index in and_indices:
        if text[index] != 'and':
            print 'Exception'
            break
        if all_txt[index - 1].lower() in local_pos and \
                        all_txt[index + 1].lower() not in local_pos and \
                        all_txt[index + 1].lower() not in local_neg and \
                        all_txt[index + 1].lower() not in NEUTRAL and \
                        all_txt[index - 1].lower() not in NEUTRAL and \
                        len(all_txt[index + 1]) > MAX_WORD_LEN:
            found_match = True
            local_pos.append(all_txt[index + 1].lower())
        if all_txt[index - 1].lower() in local_neg and \
                        all_txt[index + 1].lower() not in local_neg and \
                        all_txt[index + 1].lower() not in local_pos and \
                        all_txt[index + 1].lower() not in NEUTRAL and \
                        all_txt[index - 1].lower() not in NEUTRAL and \
                        len(all_txt[index + 1]) > MAX_WORD_LEN:
            found_match = True
            local_neg.append(all_txt[index + 1].lower())

    if found_match and False:
        # unless we are finding the match, reitereate
        return my_lexicon(local_pos, local_neg, all_txt, but_indices, and_indices)
    print 'Positive: {} \n\rwith count {}\n\rNegative: {}\n\rwith count {}'.format(local_pos, len(local_pos), local_neg,
                                                                                   len(local_neg))

    return local_pos, local_neg


p_lex = ['amaze', 'care', 'calm', 'peace', 'love']
n_lex = ['murder', 'kill']


def read_from_files():
    from os import walk
    import string
    all_text = ''
    for (dirpath, dirnames, filenames) in walk('/Users/Yasir/Downloads/review_polarity.tar/txt_sentoken/pos'):
        for filename in filenames[:1000]:
            fi = open(dirpath + '/' + filename, 'r')
            all_text += fi.read()
        break
    for (dirpath, dirnames, filenames) in walk('/Users/Yasir/Downloads/review_polarity.tar/txt_sentoken/neg'):
        for filename in filenames[:1000]:
            fi = open(dirpath + '/' + filename, 'r')
            all_text += fi.read()
        break

    all_text = all_text.translate(None, string.punctuation)
    all_text = all_text.replace('  ', ' ')
    all_txt_list = all_text.split(' ')
    but_indices = [i for i, x in enumerate(all_txt_list) if x == "but"]
    and_indices = [i for i, x in enumerate(all_txt_list) if x == "and"]

    return {'text': all_txt_list, 'but_indices': but_indices, 'and_indices': and_indices}


my_lexicon(p_lex, n_lex, **read_from_files())
