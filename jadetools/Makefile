all:	BundleScanner.py Command.py ManualBundle.py Monitoring.py ManualFiles.py Nagios.py Utils.py

BundleScanner.py:	BundleScanner.py.base CODE_utils.py IMPORT_utils.py CODE_db.py IMPORT_db.py
	python preproc.py BundleScanner.py.base

Command.py:	Command.py.base CODE_utils.py IMPORT_utils.py
	python preproc.py Command.py.base

ManualBundle.py:	ManualBundle.py.base CODE_utils.py IMPORT_utils.py
	python preproc.py ManualBundle.py.base

Monitoring.py:	Monitoring.py.base CODE_utils.py IMPORT_utils.py
	python preproc.py Monitoring.py.base

ManualFiles.py:	ManualFiles.py.base CODE_utils.py IMPORT_utils.py
	python preproc.py ManualFiles.py.base

Nagios.py:	Nagios.py.base CODE_utils.py IMPORT_utils.py
	python preproc.py Nagios.py.base

Utils.py:	Utils.py.base CODE_utils.py CODE_db.py IMPORT_utils.py IMPORT_db.py
	python preproc.py Utils.py.base
