"""
simple program to match to a given string from a given state space
"""

state_space = [
    {'y': 1, 'Y': 1},
    {'a': 2, 'A': 2},
    {'s': 3, 'S': 3},
    {'i': 4, 'I': 4},
    {'r': 5, 'R': 5}
]

def match_space(str_to_match, state_space):
    curr_index = 0
    for x in str_to_match:
        if curr_index < len(state_space):
            curr_index = state_space[curr_index].get(x)
        else:
            curr_index = 0
        if not curr_index:
            break

    if curr_index:
        print 'Our state space has matched'
        return True
    else:
        print 'Our state space has NOT matched'
        return False


assert match_space(str_to_match='Yasir', state_space=state_space)  # should match
assert match_space(str_to_match='yasir', state_space=state_space)  # should match
assert not match_space(str_to_match='aYasir', state_space=state_space)  # should not match
assert not match_space(str_to_match='Yasira', state_space=state_space)  # should not match
