# -*- coding: utf-8 -*-
def conv_tstr_to_s(input_str):
    if input_str == '--':
        return -1
    if "." in input_str:
        index = input_str.index(".", 0, len(input_str))
        input_str = input_str[0:index]
    l = input_str.split(':')
    return int(l[0]) * 60 + int(l[1])

print conv_tstr_to_s("24:55.5")