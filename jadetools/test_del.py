import os
import sys
import socket
import requests
import test.support
import unittest
import Utils as U

class DefTest(unittest.TestCase):
    #
    def test_take_release_deleter_token(self):
        ''' Try to set the token '''
        myhost = socket.gethostname().split('.')[0]
        mangled = U.mangle(myhost)
        answers = requests.post(U.targetgluedeleter + mangled)
        self.assertEqual('0', answers.text, 'Failed to get token')
        #
        answers = requests.post(U.targetgluedeleter + 'QUERY')
        testit = eval(answers.text)
        self.assertEqual(myhost, testit[0]['hostname'], 'Failed to get same host')
        #
        answers = requests.post(U.targetgluedeleter + mangled)
        self.assertEqual('1', answers.text, 'Was able to get a second token')
        #
        answers = requests.post(U.targetgluedeleter + 'QUERY')
        testit = eval(answers.text)
        self.assertEqual(myhost, testit[0]['hostname'], 'Failed to get same host')
        #
        answers = requests.post(U.targetgluedeleter + 'RELEASE')
        self.assertEqual('0', answers.text, 'Failed to RELEASE token')
        #
        answers = requests.post(U.targetgluedeleter + 'QUERY')
        self.assertEqual('[]', answers.text, 'Should have been empty')

if __name__ == "__main__":
    unittest.main()

