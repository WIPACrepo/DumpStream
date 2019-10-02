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
