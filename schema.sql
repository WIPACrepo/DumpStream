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
);
CREATE TABLE NERSCandC(
  nerscCandC_id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  localError TEXT,
  nerscError TEXT,
  nerscSize INTEGER,
  lastChangeTime TEXT
);
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
