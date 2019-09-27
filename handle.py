import json
import urllib.parse

REPLACESTRING = '+++'
def unmangle(strFromPost):
    # dummy for now.  Final thing has to fix missing spaces,
    # quotation marks, commas, slashes, and so on.
    #return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('\''', ''').replace('\@', ' ')
    return strFromPost.replace(REPLACESTRING, '/').replace('\,', ',').replace('@', ' ')

json_data="{'bundleError':''\,'bundlePoolSize':20000\,'dumpCandC_id':4\,'lastChangeTime':'2019-09-18@18:07:04'\,'status':'Drain'}"
abb=unmangle(json_data)
abc=abb.replace("\'", "\"")
print(abc)
try:
    parsed_json = (json.loads(abc))
    print(parsed_json)
except:
    print("Does not compute")

with open('x.json') as json_file:
    data = json.load(json_file)
    #for p in data:
    #    print(p)
    #print(urllib.parse.quote_plus(data['localName']))
    #print(urllib.parse.quote_plus(data['localName'], safe=""))
    #print("====")
    readytogo = urllib.parse.quote_plus(data['localName'], safe="")
    #backagain = urllib.parse.unquote_plus(readytogo, safe="")
    backagain = urllib.parse.unquote_plus(readytogo)
    print(data['localName'])
    print(backagain)
    print(readytogo)
