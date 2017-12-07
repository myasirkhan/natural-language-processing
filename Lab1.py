
def my_func(param):
    fi = open(param, "r")
    str = fi.read().split(' ')
    for word in str:
    #     str += line#.replace('.', ' <s> </s> ').replace('!', ' <s> </s> ').replace('?', ' <s> </s> ')
        print word
    print str

    # new_str = '<s>'
    # for i = 1:length(str)
    # s = str
    # {i};
    # if strcmp(s(end), '?') | | strcmp(s(end), '!')
    #     s(end) = '';
    #     if i == length(str)
    #         s = [s, ' <\s>'];
    #     else
    #         s = [s, ' <\s> <s>'];
    #     end
    # end
    #
    # if (s(end), '.') & length(s) > 3
    #     s(end) = '';
    #     if i == len(str)
    #         s = [s, ' <\s>'];
    #     else
    #         s = [s, ' <\s> <s>'];



# new_str = [new_str ' ' s]

my_func('/Users/Yasir/nlp')