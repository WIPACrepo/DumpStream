''' Not to be used against the working REST server
   I need a different curltargethost ! '''

import os
import sys
import json
import requests
import urllib
import test.support
import unittest
import Utils as U


class DefTest(unittest.TestCase):
    #
    def test_write_one(self):
        ''' Try to write a single directory '''
        dir = '/data/exp/IceCube/2031/unbiased/MDOMRaw/0231'
        mangled = U.mangle(dir)
        answers = requests.post(U.curltargethost + '/directory/' + mangled)
        self.assertEqual('', answers.text, 'Failure to insert directory')

    #
    def test_read_single(self):
        ''' Try to read a specified dirkey '''
        md = {}
        md['dirkey'] = 1
        jd = json.dumps(md)
        mangled = U.mangle(jd)
        answers = requests.get(U.curltargethost + '/directory/info/' + mangled)
        anst = '{\"results\":' + answers.text.replace('\'', '\"').replace('None', '\"\"') + '}'
        ansj = json.loads(anst)
        #print(ansj)
        firstone = ansj["results"][0]
        #print('x', firstone)
        #print(firstone['idealName'])
        self.assertEqual(firstone['idealName'], '/data/exp/IceCube/2041/unbiased/MDOMRaw/0232')
    #
    def test_read_with_like(self):
        ''' Try to read with a LIKE against idealName '''
        dir = '/data/exp/IceCube/2041/unbiased/MDOMRaw/0232'
        mangled = U.mangle(dir)
        answers = requests.post(U.curltargethost + '/directory/' + mangled)
        md = {}
        md['likeIdeal'] = 'MDOMRaw'
        mangled = U.mangle(json.dumps(md))
        answers = requests.get(U.curltargethost + '/directory/info/' + mangled)
        anst = '{\"results\":' + answers.text.replace('\'', '\"').replace('None', '\"\"') + '}'
        ansj = json.loads(anst)
        for block in ansj["results"]:
            if block['dirkey'] == 2:
                firstone = block['idealName']
        self.assertEqual(firstone, '/data/exp/IceCube/2031/unbiased/MDOMRaw/0231')
    #
    def test_read_with_status(self):
        ''' Try to select using status '''
        dir = '/data/exp/IceCube/2031/unbiased/MDOMRaw/0231'
        mangled = U.mangle(dir)
        answers = requests.post(U.curltargethost + '/directory/' + mangled)
        md = {}
        md['status'] = 'unclaimed'
        mangled = U.mangle(json.dumps(md))
        answers = requests.get(U.curltargethost + '/directory/info/' + mangled)
        anst = '{\"results\":' + answers.text.replace('\'', '\"').replace('None', '\"\"') + '}'
        ansj = json.loads(anst)
        self.assertTrue(len(ansj["results"])>1)
    #
    def test_update_simple(self):
        ''' Update with a list of simple updates, and verify that this changes '''
        FULL_DIR_SINGLE_STATI = ['unclaimed', 'filesdeleted', 'problem', 'deprecated']
        dirkey = '1'
        for status in FULL_DIR_SINGLE_STATI:
            mangled = U.mangle(dirkey + ' ' + status)
            answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
            if 'FAILURE' in answers.text:
                self.assertTrue(False)
                print(answers.text)
                continue
            answers = requests.get(U.curltargethost + '/directory/info/' + dirkey)
            anst = '{\"results\":' + answers.text.replace('\'', '\"').replace('None', '\"\"') + '}'
            ansj = json.loads(anst)
            ansa = ansj["results"]
            if len(ansa) != 1:
                self.assertTrue(False)
                continue
            self.assertEqual(status, ansa[0]["status"])
    #
    def test_update_simple_wrong_arg(self):
        ''' Update a simple update with a disallowed status '''
        dirkey = '1'
        status = 'elephant'
        mangled = U.mangle(dirkey + ' ' + status)
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        self.assertTrue('FAILURE' in answers.text)
    #
    def test_update_simple_wrong_number_args(self):
        ''' Update a simple update with too many arguments '''
        mangled = U.mangle('1 unclaimed extra')
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        self.assertTrue('FAILURE' in answers.text)
    #
    def test_update_unclear_target(self):
        ''' Update a simple update with too few arguments '''
        mangled = U.mangle('unclaimed')
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        self.assertTrue('FAILURE' in answers.text)
    #
    def test_update_processing(self):
        ''' Update to processing '''
        mangled = U.mangle('1 processing jade03 12345')
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        self.assertFalse('FAILURE' in answers.text)
        dirkey = '1'
        answers = requests.get(U.curltargethost + '/directory/info/' + dirkey)
        anst = '{\"results\":' + answers.text.replace('\'', '\"').replace('None', '\"\"') + '}'
        ansa = json.loads(anst)["results"][0]
        self.assertEqual(ansa["status"], 'processing')
    #
    def test_update_processing_wrong_number_args(self):
        ''' Update to processing, but with wrong # of args '''
        mangled = U.mangle('1 processing jade03')
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        self.assertTrue('FAILURE' in answers.text)
    #
    def test_update_request(self):
        ''' Update to LTArequest '''
        mangled = U.mangle('1 LTArequest C72A79FE-4583-44BD-AB74-B806286D3ACB')
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        dirkey = '1'
        answers = requests.get(U.curltargethost + '/directory/info/' + dirkey)
        anst = '{\"results\":' + answers.text.replace('\'', '\"').replace('None', '\"\"') + '}'
        ansa = json.loads(anst)["results"][0]
        self.assertEqual(ansa["status"], 'LTArequest')
        ansb = U.UnpackDBReturnJson(answers.text)[0]
        self.assertEqual(ansa["status"], ansb["status"])
    #
    def test_update_request_wrong_number_args(self):
        ''' Update to LTArequest with wrong number of args '''
        mangled = U.mangle('1 LTArequest')
        answers = requests.post(U.curltargethost + '/directory/modify/' + mangled)
        self.assertTrue('FAILURE' in answers.text)

        

