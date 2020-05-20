''' Read the LTA.monitor file and report to check_mk '''
#
# Format of file:
# 0 LTAM ltamonitor=2020-05-12T16:15:41.848032 OK
# 0 LTAModules - OK
# 0 LTAM ltamonitor=2020-05-12T16:15:52.883833 OK
# 0 DumpModules - OK
# 0 LTAM ltamonitor=2020-05-12T16:15:52.883844 OK
# 0 BundleStatus - OK
# 0 LTAM ltamonitor=2020-05-12T16:15:52.883850 OK
# #
# output format like
#   root@linux# /usr/lib/check_mk_agent/local/mycustomscript
# 0 myservice count1=42|count2=21;23;27|count3=73 OK - This is my custom output
import sys
import datetime

thisrun = datetime.datetime.now()

try:
    fhandle = open('LTA.monitor', 'r')
except:
    print('2 LTAMonitor - CRIT - Cannot open LTA.monitor')
    sys.exit(2)

try:
    contents = fhandle.readlines()
except:
    print('2 LTAMonitor - CRIT - Cannot read LTA.monitor')
    sys.exit(2)
fhandle.close()

if len(sys.argv) <= 1:
    number_lines = len(contents)
    last_line_words = contents[number_lines-1].strip().split()
    old_time = last_line_words[-2].split('=')[1]
    old_internal = datetime.datetime.strptime(old_time, '%Y-%m-%dT%H:%M:%S.%f')
    difft = (thisrun - old_internal)
    delay = difft.days*86400 + difft.seconds
    if delay > 3600:
        print('2 LTAMonitor - CRIT - Monitor not running')
        sys.exit(2)
    print('0 LTAMonitor - OK')
    sys.exit(0)

if 'LTA' in sys.argv:
    ww = contents[1].split()
    try:
        icode = int(ww[0])
    except:
        icode = 2
    print(contents[1].strip())
    sys.exit(icode)

if 'Dump' in sys.argv:
    ww = contents[3].split()
    try:
        icode = int(ww[0])
    except:
        icode = 2
    print(contents[3].strip())
    sys.exit(icode)

if 'Bundle' in sys.argv:
    ww = contents[5].split()
    try:
        icode = int(ww[0])
    except:
        icode = 2
    print(contents[5].strip())
    sys.exit(icode)

if 'Interface' in sys.argv:
    ww = contents[7].split()
    try:
        icode = int(ww[0])
    except:
        icode = 2
    print(contents[7].strip())
    sys.exit(icode)

sys.exit(0)
