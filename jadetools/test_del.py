import os
import sys
import socket
import requests
import Utils as U

def tryset():
    ''' Try to set the token '''
    answers = requests.post(U.targetgluedeleter + U.mangle(socket.gethostname().split('.')[0]))
    print(answers.text)
    answers = requests.post(U.targetgluedeleter + 'QUERY')
    print(answers.text)
    answers = requests.post(U.targetgluedeleter + U.mangle(socket.gethostname().split('.')[0]))
    print(answers.text)
    answers = requests.post(U.targetgluedeleter + 'QUERY')
    print(answers.text)
    answers = requests.post(U.targetgluedeleter + 'RELEASE')
    print(answers.text)
    answers = requests.post(U.targetgluedeleter + 'QUERY')
    print(answers.text)

tryset()
