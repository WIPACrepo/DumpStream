REPLACESTRING = '+++'
def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('@', ' ')

def mangle(strFromPost):
    # Remote jobs will use this more than we will here.
    return strFromPost.replace('/', REPLACESTRING).replace(',', '\,').replace(' ', '@')

trial = 'HPSS Not available'
xtrial = 'HPSS@Not@available'

print(mangle(trial))
print(unmangle(xtrial))
