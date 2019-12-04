all:	NERSC_Client.py CODE_utils.py

NERSC_Client.py:	NERSC_Client.py.base CODE_utils.py IMPORT_utils.py
	python preproc.py NERSC_Client.py.base

CODE_utils.py:	../setup
	../setup
