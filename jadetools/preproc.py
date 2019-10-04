########
#
import sys

filename = str(sys.argv[1])
if '.base' not in filename:
    print('Wrong type of file', filename)
    sys.exit(1)

try:
    curin = open(filename, 'r')
except:
    print('Cannot open', filename)
    sys.exit(1)

newfile = filename.replace('.base', '')

try:
    curout = open(newfile, 'w')
except:
    print('Cannot open for write', newfile)
    sys.exit(1)

wholeinput = curin.readlines()
curin.close()

for line in wholeinput:
    if '#SUPERIMPORT' not in line:
        curout.write(line)
        continue
    words = line.strip().split()
    try:
        newin = open(words[1], 'r')
    except:
        print('Cannot open imported file', newin)
        sys.exit(1)
    newincl = newin.readlines()
    newin.close()
    for newline in newincl:
        curout.write(newline)

curout.close()
