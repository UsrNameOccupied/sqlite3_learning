/*
** 2018-08-11, base on src/test3.c
*/
#include "sqliteInt.h"
#include "btree.h"
#include "btreeInt.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
** Return a static string that describes the kind of error specified in the
** argument.
*/
const char *sqlite3ErrStr(int rc){
  static const char* const aMsg[] = {
    /* SQLITE_OK          */ "not an error",
    /* SQLITE_ERROR       */ "SQL logic error",
    /* SQLITE_INTERNAL    */ 0,
    /* SQLITE_PERM        */ "access permission denied",
    /* SQLITE_ABORT       */ "query aborted",
    /* SQLITE_BUSY        */ "database is locked",
    /* SQLITE_LOCKED      */ "database table is locked",
    /* SQLITE_NOMEM       */ "out of memory",
    /* SQLITE_READONLY    */ "attempt to write a readonly database",
    /* SQLITE_INTERRUPT   */ "interrupted",
    /* SQLITE_IOERR       */ "disk I/O error",
    /* SQLITE_CORRUPT     */ "database disk image is malformed",
    /* SQLITE_NOTFOUND    */ "unknown operation",
    /* SQLITE_FULL        */ "database or disk is full",
    /* SQLITE_CANTOPEN    */ "unable to open database file",
    /* SQLITE_PROTOCOL    */ "locking protocol",
    /* SQLITE_EMPTY       */ 0,
    /* SQLITE_SCHEMA      */ "database schema has changed",
    /* SQLITE_TOOBIG      */ "string or blob too big",
    /* SQLITE_CONSTRAINT  */ "constraint failed",
    /* SQLITE_MISMATCH    */ "datatype mismatch",
    /* SQLITE_MISUSE      */ "bad parameter or other API misuse",
#ifdef SQLITE_DISABLE_LFS
    /* SQLITE_NOLFS       */ "large file support is disabled",
#else
    /* SQLITE_NOLFS       */ 0,
#endif
    /* SQLITE_AUTH        */ "authorization denied",
    /* SQLITE_FORMAT      */ 0,
    /* SQLITE_RANGE       */ "column index out of range",
    /* SQLITE_NOTADB      */ "file is not a database",
    /* SQLITE_NOTICE      */ "notification message",
    /* SQLITE_WARNING     */ "warning message",
  };
  const char *zErr = "unknown error";
  switch( rc ){
    case SQLITE_ABORT_ROLLBACK: {
      zErr = "abort due to ROLLBACK";
      break;
    }
    case SQLITE_ROW: {
      zErr = "another row available";
      break;
    }
    case SQLITE_DONE: {
      zErr = "no more rows available";
      break;
    }
    default: {
      rc &= 0xff;
      if( ALWAYS(rc>=0) && rc<ArraySize(aMsg) && aMsg[rc]!=0 ){
        zErr = aMsg[rc];
      }
      break;
    }
  }
  return zErr;
}

/*
** A bogus sqlite3 connection structure for use in the btree
** tests.
*/
static sqlite3 sDb;
static int nRefSqlite3 = 0;

/*
** Usage:   btree_open FILENAME NCACHE
**
** Open a new database
*/
static int SQLITE_TCLAPI btree_open(const char *zFilename, Btree **pBt){
  int rc, nCache = 100;

  nRefSqlite3++;
  if( nRefSqlite3==1 ){
    sDb.pVfs = sqlite3_vfs_find(0);
    sDb.mutex = sqlite3MutexAlloc(SQLITE_MUTEX_RECURSIVE);
    sqlite3_mutex_enter(sDb.mutex);
  }

  rc = sqlite3BtreeOpen(sDb.pVfs, zFilename, &sDb, pBt, 0, 
     SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_URI);

  if( rc!=SQLITE_OK ){
	printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }

  sqlite3BtreeSetCacheSize(*pBt, nCache);
  // (*pBt)->inTrans = TRANS_WRITE;

  printf("btree addr: %p\n", *pBt);

  return 0;
}

/*
** Usage:   btree_close ID
**
** Close the given database.
*/
static int SQLITE_TCLAPI btree_close(Btree *pBt){
  int rc;

  rc = sqlite3BtreeClose(pBt);
  if( rc!=SQLITE_OK ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }
  
  nRefSqlite3--;

  if( nRefSqlite3==0 ){
    sqlite3_mutex_leave(sDb.mutex);
    sqlite3_mutex_free(sDb.mutex);
    sDb.mutex = 0;
    sDb.pVfs = 0;
  }

  return 0;
}


/*
** Usage:   btree_begin_transaction ID
**
** Start a new transaction
*/
static int SQLITE_TCLAPI btree_begin_transaction(Btree *pBt){
  int rc;
  
  sqlite3BtreeEnter(pBt);
  rc = sqlite3BtreeBeginTrans(pBt, 1);
  sqlite3BtreeLeave(pBt);

  if( rc!=SQLITE_OK ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }

  return 0;
}

/*
** Usage:   btree_cursor ID TABLENUM WRITEABLE
**
** Create a new cursor.  Return the ID for the cursor.
*/
static int SQLITE_TCLAPI btree_cursor(Btree *pBt, int iTable, BtCursor **pCur){
  int rc = SQLITE_OK;
  int wrFlag = 1;

  if( wrFlag ) wrFlag = BTREE_WRCSR;
  *pCur = (BtCursor *)malloc(sqlite3BtreeCursorSize());
  memset(*pCur, 0, sqlite3BtreeCursorSize());
  sqlite3_mutex_enter(pBt->db->mutex);
  sqlite3BtreeEnter(pBt);
  
  rc = sqlite3BtreeBeginTrans(pBt, wrFlag);
  if( rc ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }
#ifndef SQLITE_OMIT_SHARED_CACHE
  rc = sqlite3BtreeLockTable(pBt, iTable, !!wrFlag);
#endif
  if( rc==SQLITE_OK ){
    rc = sqlite3BtreeCursor(pBt, iTable, wrFlag, 0, *pCur);
  }
  sqlite3BtreeLeave(pBt);
  sqlite3_mutex_leave(pBt->db->mutex);
  if( rc ){
    free((char *)(*pCur));
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }

  printf("cursor addr: %p\n", *pCur);

  return SQLITE_OK;
}

/*
** Usage:   btree_close_cursor ID
**
** Close a cursor opened using btree_cursor.
*/
static int SQLITE_TCLAPI btree_close_cursor(BtCursor *pCur){
  int rc;

#if SQLITE_THREADSAFE>0
  {
    Btree *pBt = pCur->pBtree;
    sqlite3_mutex_enter(pBt->db->mutex);
    sqlite3BtreeEnter(pBt);
    rc = sqlite3BtreeCloseCursor(pCur);
    sqlite3BtreeLeave(pBt);
    sqlite3_mutex_leave(pBt->db->mutex);
  }
#else
  rc = sqlite3BtreeCloseCursor(pCur);
#endif
  free((char *)pCur);

  if( rc ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }
  
  return SQLITE_OK;
}

/*
** Usage:   btree_next ID
**
** Move the cursor to the next entry in the table.  Return 0 on success
** or 1 if the cursor was already on the last entry in the table or if
** the table is empty.
*/
static int SQLITE_TCLAPI btree_next(BtCursor *pCur){
  int rc;
  int res = 0;

  sqlite3BtreeEnter(pCur->pBtree);
  rc = sqlite3BtreeNext(pCur, 0);
  if( rc==SQLITE_DONE ){
    res = 1;
    rc = SQLITE_OK;
  }

  sqlite3BtreeLeave(pCur->pBtree);

  if( rc ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }

  return res;
}

/*
** Usage:   btree_first ID
**
** Move the cursor to the first entry in the table.  Return 0 if the
** cursor was left point to something and 1 if the table is empty.
*/
static int SQLITE_TCLAPI btree_first(BtCursor *pCur){
  int rc;
  int res = 0;

  sqlite3BtreeEnter(pCur->pBtree);
  rc = sqlite3BtreeFirst(pCur, &res);
  sqlite3BtreeLeave(pCur->pBtree);

  if( rc ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }

  return res;
}

/*
** Usage:   btree_eof ID
**
** Return TRUE if the given cursor is not pointing at a valid entry.
** Return FALSE if the cursor does point to a valid entry.
*/
static int SQLITE_TCLAPI btree_eof(BtCursor *pCur){
  int rc;

  sqlite3BtreeEnter(pCur->pBtree);
  rc = sqlite3BtreeEof(pCur);
  sqlite3BtreeLeave(pCur->pBtree);

  return rc;
}

/*
** Usage:   btree_payload_size ID
**
** Return the number of bytes of payload
*/
static int SQLITE_TCLAPI btree_payload_size(BtCursor *pCur){
  u32 n;

  sqlite3BtreeEnter(pCur->pBtree);
  n = sqlite3BtreePayloadSize(pCur);
  sqlite3BtreeLeave(pCur->pBtree);

  return n;
}

/*
** usage:   btree_insert CSR ?KEY? VALUE
**
** Set the size of the cache used by btree $ID.
*/
static int SQLITE_TCLAPI btree_insert(BtCursor *pCur, int key, void* value, int size){
  int rc;
  BtreePayload x;

  memset(&x, 0, sizeof(x));

  x.nKey = key;
  x.pData = value;
  x.nData = size;

  sqlite3_mutex_enter(pCur->pBtree->db->mutex);
  sqlite3BtreeEnter(pCur->pBtree);
  rc = sqlite3BtreeInsert(pCur, &x, 0, 0);
  sqlite3BtreeLeave(pCur->pBtree);
  sqlite3_mutex_leave(pCur->pBtree->db->mutex);

  if( rc ){
    printf("%s\n", sqlite3ErrStr(rc));
    return -1;
  }

  return 0;
}

static int btree_commit(Btree *pBt) {
  return sqlite3BtreeCommit(pBt);
}

/*
** Register commands with the TCL interpreter.
*/
int main(int agrc, char* argv[]) {
  int ret;
  Btree *pBt;
  BtCursor *pCur;

  // create an empty datebase file and exec following sqls.
  // CREATE TABLE x1(a INTEGER PRIMARY KEY, varchar b);
  // insert into x1 values(1,"Hey man!");
  // insert into x1 values(2,"Hey man!");
  // insert into x1 values(3,"Hey man!");
  // insert into x1 values(4,"Hey man!");

  int iTable = 2;

  // The Serial Type Code is used to denote the type of data found in a field within the payload and it's length.
  // |----------------------------------------------------------------------------------------------|
  // | payload header length(4) | Serial Type Code(0) | Serial Type Code(27) | Serial Type Code(27) |
  // |----------------------------------------------------------------------------------------------|
  
  int size = 0;
  char payload[120] = {0};
  
  // only two fields here,including payload header length
  payload[size] = 3;
  size += 1;
  
  // INTEGER PRIMARY KEY
  payload[size] = 0;
  size += 1;

  payload[size] = (strlen("Hello!") * 2) + 13;
  size += 1;
  
  snprintf(payload + size, sizeof(payload) - size, "%s", "Hello!");
  size += strlen("Hello!");
  
  btree_open("./test.db", &pBt);
  btree_cursor(pBt, iTable, &pCur);
  btree_insert(pCur, 5, payload, size);
  btree_close_cursor(pCur);
  btree_commit(pBt);

  return 0;
}

