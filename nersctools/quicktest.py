import SYTools
import socket
SYTools.LOGFILE = 'logg'

#h=socket.gethostname().split('.')
#print(h[0])

answer, errors, code = SYTools.getoutputerrorsimplecommand(['/home/jbellinger/archivecontrol/nersctools/gobad'])
print(str(answer))
print(str(errors))
print(str(code))
print('==')

answer, errors, code = SYTools.getoutputerrorsimplecommand(['/home/jbellinger/archivecontrol/nersctools/gogood'])
print(str(answer))
print(str(errors))
print(str(code))
