import sys
import requests
#import Utils as U


class CandC():
    ''' System to let manager control the dump
        Note that throttling is separate, and not controllable
        by this system '''
    def __init__(self):
        self.DumperStatusOptions = ['Idle', 'Dumping', 'Inventorying', 'Error']
        # Note: Added operator functions, remove DumpOne (a legit command)
        self.DumperNextOptions = ['Dump', 'Pause', 'Inventory', 'Help', 'Status']
        self.baseurl = 'http://archivecontrol.wipac.wisc.edu'
    #
    def BasicOutput(self):
        ''' Tell the user what is going on, and ask what to do next '''
        nowAction, nextAction = self.DumperTodo()
        print('The dump action last time we checked was:', nowAction)
        print('The dump action next in line is:', nextAction)
        print(' Options for controlling the next dump run are:')
        print('Do what?  Dump   Pause   Inventory  Help  Status')
        print('No answer => change nothing')
    #
    def HelpOutput(self):
        ''' Describe what the commands do '''
        print('')
        print('Commands become effective the next time DumpControl runs')
        print(' Later changes can override this one, up until the run')
        print('   Dump:  DumpControl should dump the next disk in order')
        print('         and then keep dumping until told to stop')
        print('   Pause:  DumpControl should do no dump until further notice')
        print('         Active jobs will complete normally')
        print('         Non-dump runs do the checks on full directories')
        print('   Inventory:  After loading new disks, use this to tell DumpControl')
        print('         to run the Inventory, and then Pause.')
        print('   Help:  Prints this message')
        print('   nothing, a bare return:  Exits doing nothing')
        print('Optionally, you can specify the command on the command line')
    #
    def PrintStatus(self):
        ''' Retrieve and print the current DumpControl status,
            as of the last run '''
        dumpstatus, dumpNextAction = self.DumperTodo()
        print('Status is as of last DumpControl run')
        print(' Current action = ', dumpstatus)
        print(' Next action = ', dumpNextAction)
    #
    def DumperTodo(self):
        ''' Return what the current and next commands are '''
        answer = requests.get(self.baseurl + '/dumping/state')
        try:
            js = answer.json()
        except:
            print('CandC:DumperTodo failed to parse', answer.text)
            return None, None
        return js['nextAction'], js['status']
    #
    def DumperSetNext(self, command):
        ''' Set line in REST server DB for DumpControl next state  after this '''
        if command not in ['Dump', 'Pause', 'Inventory']:
            print('CandC:DumperSetState invalid command', command)
            return
        answer = requests.post(self.baseurl + '/dumping/state/nextaction/' + command)
        if 'FAILURE' in answer:
            print('CandC:DumperSetState failed with command', command)
    #
    def DumperSetState(self, command):
        ''' Set line in REST server DB for DumpControl next state '''
        if command not in ['Dump', 'Pause', 'DumpOne', 'Inventory']:
            print('CandC:DumperSetState invalid command', command)
            return
        answer = requests.get(self.baseurl + '/dumping/state/status/' + command)
        if 'FAILURE' in answer:
            print('CandC:DumperSetState failed with command', command)
    #
    def Communicate(self):
        ''' Interact with the user '''
        #
        if len(sys.argv) == 2:
            tentativeCommand = str(sys.argv[1])
            if tentativeCommand not in self.DumperNextOptions:
                print('I do not recognize the command', tentativeCommand)
                self.HelpOutput()
                sys.exit(1)
            if tentativeCommand == 'Help':
                self.HelpOutput()
                sys.exit(0)
            if tentativeCommand == 'Status':
                self.PrintStatus()
                sys.exit(0)
            self.DumperSetState(tentativeCommand)
            if tentativeCommand == 'Inventory':
                self.DumperSetNext('Pause')
            sys.exit(0)
        #
        self.BasicOutput()
        #
        try:
            myread = str(input('> ')).strip()
        except:
            sys.exit(0)
        if len(myread) <= 0:
            sys.exit(0)
        if myread not in self.DumperNextOptions or myread == 'Help':
            self.HelpOutput()
            sys.exit(0)
        if myread == 'Status':
            self.PrintStatus()
            sys.exit(0)
        if myread == 'Inventory':
            self.DumperSetNext('Pause')
        self.DumperSetNext(myread)
        sys.exit(0)

if __name__ == "__main__":
    candc = CandC()
    candc.Communicate()
