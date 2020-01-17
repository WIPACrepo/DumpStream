CREATE TABLE BundleStatus(
  bundleStatus_id INTEGER PRIMARY KEY,
  localName TEXT NOT NULL,
  idealName TEXT,
  UUIDJade TEXT,
  UUIDGlobus TEXT,
  size INTEGER,
  status TEXT NOT NULL,
  useCount INTEGER,
  checksum TEXT
, LooseFileDir TEXT, DownloadDir TEXT, FileDate TEXT);
CREATE TABLE NERSCandC(
  nerscCandC_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  localError TEXT,
  nerscError TEXT,
  nerscSize INTEGER,
  lastChangeTime TEXT
, hpsstree INTEGER);
CREATE TABLE DumpCandC(
  dumpCandC_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  bundlePoolSize INTEGER,
  bundleError TEXT,
  lastChangeTime TEXT
);
CREATE TABLE Heartbeats(
  hostname TEXT,
  lastChangeTime TEXT
);
CREATE TABLE Token(
  hostname TEXT,
  lastChangeTime TEXT
);
CREATE TABLE Trees(
  tree_id INTEGER PRIMARY KEY,
  ultimate TEXT,
  treetop TEXT
);
CREATE TABLE SlotContents(
  slotnumber  INTEGER NOT NULL,
  name   TEXT,
  poledisk_id   INTEGER
);
CREATE TABLE DumpTarget(
  target      TEXT
);
CREATE TABLE OldDumpTargets(
  oldtarget_id  INTEGER PRIMARY KEY,
  target      TEXT
);
CREATE TABLE DumpSystemState(
  dumpss_id   INTEGER PRIMARY KEY,
  nextAction  TEXT,
  status      TEXT,
  lastChangeTime  TEXT,
  count       INTEGER
);
CREATE TABLE PoleDisk(
  poledisk_id	INTEGER PRIMARY KEY,
  diskuuid    TEXT,
  slotnumber  INTEGER,
  dateBegun   TEXT,
  dateEnded   TEXT,
  targetArea  TEXT,
  status      TEXT
);
CREATE TABLE WantedTrees (
  wantedtree_id INTEGER PRIMARY KEY,
  dataTree   TEXT
);
CREATE TABLE BundleTreesWanted (
  wantedtree_id INTEGER PRIMARY KEY,
  dataTree   TEXT,
  bundletype TEXT,
  subdirs    INTEGER
);
CREATE TABLE expected (directory TEXT PRIMARY KEY, number INTEGER, done INTEGER);
CREATE TABLE FullDirectories (
  idealName  TEXT PRIMARY KEY,
  toLTA   INTEGER
);
CREATE TABLE NERSCHPSS (
  hpss_id     INTEGER PRIMARY KEY,
  TreeTopName TEXT
);
CREATE TABLE GlueStatus (
   status TEXT,
   lastChangeTime  TEXT
);
CREATE TABLE GlueDump ( type TEXT PRIMARY KEY, lastChangeTime TEXT);
CREATE TABLE LTACatalog (idealName TEXT PRIMARY KEY, status TEXT, condorjob INTEGER);
CREATE TABLE ActiveDirectory (idealDir TEXT PRIMARY KEY, lastChangeTime TEXT);
CREATE TABLE WorkingTable ( idealDir TEXT PRIMARY KEY, status TEXT);

