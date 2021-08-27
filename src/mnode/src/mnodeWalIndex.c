/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include "tfile.h"
#include "cache.h"
#include "hash.h"
#include "mnodeSdb.h"
#include "mnodeInt.h"
#include "twal.h"
#include "tglobal.h"
#include "taoserror.h"
#include "mnodeSdbTable.h"
#include "mnodeWalIndex.h"

extern uint64_t tsSdbIndexVersion;;

typedef walIndex* (*decodeParentKeyFp)(uint8_t keyLen, void* pHead);

extern walIndex* mnodeVgroupDecodeParentKey(uint8_t keyLen, void* pArg);
extern walIndex* mnodeSTableDecodeParentKey(uint8_t keyLen, void* pArg);
extern walIndex* mnodeCTableDecodeParentKey(uint8_t keyLen, void* pArg);

static decodeParentKeyFp tsTableDecodeParentKeyFp[] = {
  NULL, // SDB_TABLE_CLUSTER
  NULL, // SDB_TABLE_DNODE
  NULL, // SDB_TABLE_MNODE
  NULL, // SDB_TABLE_ACCOUNT
  NULL, // SDB_TABLE_USER
  NULL, // SDB_TABLE_DB
  mnodeVgroupDecodeParentKey, // SDB_TABLE_VGROUP
  mnodeSTableDecodeParentKey, // SDB_TABLE_STABLE
  mnodeCTableDecodeParentKey, // SDB_TABLE_CTABLE,
  NULL, // SDB_TABLE_FUNC
};

typedef int32_t (*fpDecode)(SSdbRow *pRow);
extern int32_t mnodeDbActionDecode(SSdbRow *pRow);
extern int32_t mnodeSuperTableActionDecode(SSdbRow *pRow);

static fpDecode tsParentTableDecodeFp[] = {
  NULL, // SDB_TABLE_CLUSTER
  NULL, // SDB_TABLE_DNODE
  NULL, // SDB_TABLE_MNODE
  NULL, // SDB_TABLE_ACCOUNT
  NULL, // SDB_TABLE_USER
  mnodeDbActionDecode, // SDB_TABLE_DB
  NULL, // SDB_TABLE_VGROUP
  mnodeSuperTableActionDecode, // SDB_TABLE_STABLE
  NULL, // SDB_TABLE_CTABLE,
  NULL, // SDB_TABLE_FUNC
};

typedef bool (*deleteByParentFp)(void* pHead, void* pIndex);

extern bool mnodeDeleteVgroupIndexByDb(void* pArg, void* pIndex);
extern bool mnodeDeleteSTableIndexByDb(void* pArg, void* pIndex);
extern bool mnodeDeleteCTableIndexBySTable(void* pArg, void* pIndex);

static deleteByParentFp tsDeleteByParentFp[] = {
  NULL, // SDB_TABLE_CLUSTER
  NULL, // SDB_TABLE_DNODE
  NULL, // SDB_TABLE_MNODE
  NULL, // SDB_TABLE_ACCOUNT
  NULL, // SDB_TABLE_USER
  NULL, // SDB_TABLE_DB
  mnodeDeleteVgroupIndexByDb, // SDB_TABLE_VGROUP
  mnodeDeleteSTableIndexByDb, // SDB_TABLE_STABLE
  mnodeDeleteCTableIndexBySTable, // SDB_TABLE_CTABLE,
  NULL, // SDB_TABLE_FUNC
};

static int32_t tsParentTableId[] = {
  -1, // SDB_TABLE_CLUSTER
  -1, // SDB_TABLE_DNODE
  -1, // SDB_TABLE_MNODE
  -1, // SDB_TABLE_ACCOUNT
  -1, // SDB_TABLE_USER
  -1, // SDB_TABLE_DB
  SDB_TABLE_DB, // SDB_TABLE_VGROUP
  SDB_TABLE_DB, // SDB_TABLE_STABLE
  SDB_TABLE_STABLE, // SDB_TABLE_CTABLE,
  -1, // SDB_TABLE_FUNC
};

typedef struct walIndexFileInfo {
  char name[TSDB_FILENAME_LEN];

  uint64_t version;
  int64_t offset;
  int64_t total;
  int64_t tfd;

  struct walIndexFileInfo* next;

  walIndexItem* tail[SDB_TABLE_MAX];
  walIndexItem* head[SDB_TABLE_MAX];

  int64_t tableSize[SDB_TABLE_MAX];
} walIndexFileInfo;

static SHashObj* tsIndexItemTable[SDB_TABLE_MAX];
static walIndexFileInfo* tsWalFileInfo;
static walIndexFileInfo* tsCurrentWalFileInfo;

// index data type

typedef enum walIndexContentType {
  WAL_INDEX_HEADER        = 0,
  WAL_INDEX_TABLE_HEADER  = 1,
  WAL_INDEX_ITEM          = 2,
} walIndexContentType;

#pragma  pack(1)
typedef struct walIndexHeaderMeta {
  walIndexContentType type;
  int64_t totalSize;
  uint64_t version;
  int64_t offset;
  int32_t walNameSize;
} walIndexHeaderMeta;

#pragma  pack(1)
typedef struct walIndexHeader {
  walIndexHeaderMeta meta;

  char walName[];
} walIndexHeader;

#pragma  pack(1)
typedef struct walIndexTableHeader {
  walIndexContentType type;
  ESdbTable tableId;
  int64_t tableSize;
} walIndexTableHeader;

static void deleteIndex(walIndexItem* pItem, SWalHead *pHead, int32_t tableId, bool needFree);

static void deleteChildIndex(walIndexItem* pParentItem, SWalHead *pHead, int32_t parentTableId) {
  if (tsParentTableDecodeFp[parentTableId] == NULL) {
    return;
  }
  bool newHead = false;
  if (!pHead) {
    pHead = walReadAt(pParentItem->pFileInfo->tfd, pParentItem->pIndex->offset, pParentItem->pIndex->size);
    if (!pHead) {
      return;
    }
    newHead = true;
  }

  SSdbRow row = (SSdbRow){.rowSize = pHead->len, .rowData = pHead->cont};

  int32_t code = (*tsParentTableDecodeFp[parentTableId])(&row);
  if (code != 0) {
    return;
  }

  int childTableId = 0;
  for (childTableId = 0; childTableId < SDB_TABLE_MAX; ++childTableId) {
    if (parentTableId != tsParentTableId[childTableId]) {
      continue;
    }

    walIndexFileInfo* pFileInfo = tsWalFileInfo;
    while (pFileInfo != NULL) {
      walIndexItem *p = pFileInfo->head[childTableId];
      while (p) {
        walIndexItem* next = p->next;
        if (tsDeleteByParentFp[childTableId](row.pObj, p->pIndex)) {
          //sdbInfo("delete child %d %s", childTableId, p->index.key);
          deleteIndex(p, NULL, childTableId, true);
        }
        p = next;
      }

      pFileInfo = pFileInfo->next;
    }
  }

  if (newHead) free(pHead);
  free(row.pObj);
}

static void deleteIndex(walIndexItem* pItem, SWalHead *pHead, int32_t tableId, bool needFree) {
  walIndexFileInfo *pFileInfo = pItem->pFileInfo;
  walIndexItem* prev = pItem->prev;
  walIndexItem* next = pItem->next;

/*
  if (tableId == SDB_TABLE_DB || tableId == SDB_TABLE_STABLE)
    sdbInfo("delete %d %s", tableId, pItem->index.key);
*/

  if (pFileInfo->tail[tableId] == pItem) {
    pFileInfo->tail[tableId] = prev;
  }
  if (pFileInfo->head[tableId] == pItem) {
    pFileInfo->head[tableId] = next;
  }
  if (prev) prev->next = next;
  if (next) next->prev = prev;  

  if (needFree) {
    deleteChildIndex(pItem, pHead, tableId);

    pFileInfo->total -= sizeof(walIndex) + pItem->pIndex->keyLen + pItem->pIndex->parentKeyLen;
    pFileInfo->tableSize[tableId] -= sizeof(walIndex) + pItem->pIndex->keyLen + pItem->pIndex->parentKeyLen;
    assert(pFileInfo->total >= 0);

    free(pItem);
  }  
}

static void linkToList(walIndexItem* pItem, int32_t tableId) {
  walIndexFileInfo *pFileInfo = tsCurrentWalFileInfo;

  if (pFileInfo->tail[tableId] == NULL) {      
    pFileInfo->tail[tableId] = pItem;      
  } else {
    pItem->prev = tsWalFileInfo[0].tail[tableId];
    pFileInfo->tail[tableId]->next = pItem;
    pFileInfo->tail[tableId] = pItem;
  }
  if (pFileInfo->head[tableId] == NULL) {
    pFileInfo->head[tableId] = pItem;   
  }  
}

static int32_t buildIndex(void *wparam, void *hparam, int32_t qtype, void *tparam, void* head) {
  //SSdbRow *pRow = wparam;
  SWalHead *pHead = hparam;
  SWalHeadInfo* pHeadInfo = (SWalHeadInfo*)head;
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;
  walIndexFileInfo *pFileInfo = tsCurrentWalFileInfo;
  ESdbKey keyType = sdbGetKeyTypeByTableId(tableId);

  SHashObj* pTable = tsIndexItemTable[tableId];

  int32_t keySize;
  void* key = sdbTableGetKeyAndSize(keyType, pHead->cont, &keySize, false);

  // update or delete action will delete old index item
  if (action == SDB_ACTION_UPDATE || action == SDB_ACTION_DELETE) {
    walIndexItem** ppItem = (walIndexItem**)taosHashGet(pTable, key, keySize);
    deleteIndex(*ppItem, pHead, tableId, action == SDB_ACTION_DELETE);
    if (action == SDB_ACTION_UPDATE) {
      (*ppItem)->pIndex->offset = pHeadInfo->offset;
      (*ppItem)->pIndex->size   = pHeadInfo->len;
      linkToList(*ppItem, tableId);
    }
  }

  // insert or update action will save index item
  if (action == SDB_ACTION_INSERT) {
    walIndex *pIndex = NULL;
    if (tsTableDecodeParentKeyFp[tableId] != NULL) {
      pIndex = tsTableDecodeParentKeyFp[tableId](keySize, pHead);
    } else {
      pIndex = calloc(1, sizeof(walIndex) + keySize);
    }
    if (pIndex == NULL) {
      return -1;
    }
    pIndex->keyLen = keySize;
    pIndex->offset = pHeadInfo->offset;
    pIndex->size   = pHeadInfo->len;
    memcpy(pIndex->data, key, keySize);

    walIndexItem* pItem = calloc(1, sizeof(walIndexItem));
    *pItem = (walIndexItem) {
      .needFreeIndex = true,
      .pIndex = pIndex,
      .next = NULL,
      .prev = NULL,
      .pFileInfo = pFileInfo,
    };
    
    linkToList(pItem, tableId);

    taosHashPut(pTable, key, keySize, &pItem, sizeof(walIndexItem**));
    pFileInfo->total += sizeof(walIndex) + keySize + pItem->pIndex->parentKeyLen;
    pFileInfo->tableSize[tableId] += sizeof(walIndex) + keySize + pItem->pIndex->parentKeyLen;
  }

  if (pHeadInfo->offset + pHeadInfo->len > pFileInfo->offset) {
    pFileInfo->offset = pHeadInfo->offset + pHeadInfo->len;
  }

  if (pHead->version > pFileInfo->version) {
    pFileInfo->version = pHead->version;
    tsSdbIndexVersion = pHead->version;
  }

  return 0;
}

static walIndexFileInfo* createWalIndexFileInfo(const char* walName) {
  walIndexFileInfo* pIndexFileInfo = calloc(1, sizeof(walIndexFileInfo));
  strcpy(pIndexFileInfo->name, walName);
  pIndexFileInfo->tfd = tfOpen(walName, O_RDWR);
  if (!tfValid(pIndexFileInfo->tfd)) {
    free(pIndexFileInfo);
    return NULL;
  }
  return pIndexFileInfo;
}

static void beginBuildWalIndex(const char* walName) {
  walIndexFileInfo* pIndexFileInfo = createWalIndexFileInfo(walName);
  if (!pIndexFileInfo) {
    return;
  }

  if (tsWalFileInfo == NULL) {
    tsWalFileInfo = pIndexFileInfo;
  } else {
    assert(tsCurrentWalFileInfo != NULL);
    tsCurrentWalFileInfo->next = pIndexFileInfo;
  }

  tsCurrentWalFileInfo = pIndexFileInfo;

  sdbInfo("file:%s, open for build index", walName);
}

static void addExistIndex(ESdbTable tableId, walIndex* pIndex) {
  walIndexFileInfo *pFileInfo = tsCurrentWalFileInfo;
  //ESdbKey keyType = sdbGetKeyTypeByTableId(tableId);
  SHashObj* pTable = tsIndexItemTable[tableId];

  int32_t keySize = pIndex->keyLen;
  void* key = pIndex->data;

  walIndexItem* pItem = calloc(1, sizeof(walIndexItem));
  *pItem = (walIndexItem) {
    .needFreeIndex = false,
    .pIndex = pIndex,
    .next = NULL,
    .prev = NULL,
    .pFileInfo = pFileInfo,
  };

  linkToList(pItem, tableId);

  taosHashPut(pTable, key, keySize, &pItem, sizeof(walIndexItem**));
  pFileInfo->total += sizeof(walIndex) + pIndex->keyLen + pIndex->parentKeyLen;
  pFileInfo->tableSize[tableId] += sizeof(walIndex) + pIndex->keyLen + pIndex->parentKeyLen;
}

// read index file and return buffer
static walIndexHeader* readIndex(char** pSave) {
  char    name[TSDB_FILENAME_LEN] = {0};
  sprintf(name, "%s/wal/index", tsMnodeDir);  
  int64_t tfd = tfOpen(name, O_RDONLY);
  if (!tfValid(tfd)) {
    sdbInfo("file:%s, failed to open index since %s, restore from wal", name, strerror(errno));
    // no index, restore from wal files
    return NULL;
  }

  int64_t size = tfLseek(tfd, 0, SEEK_END);
  if (size <= 0) {
    sdbInfo("file:%s, failed to open index since %s, restore from wal", name, strerror(errno));
    // no index, restore from wal files
    return NULL;
  }

  char *buffer = calloc(1, size);
  if (buffer == NULL) {
    return NULL;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  tfLseek(tfd, 0, SEEK_SET);
  if (tfRead(tfd, buffer, size) != size) {
    sdbError("file:%s, failed to read since %s", name, strerror(errno));
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  *pSave = buffer;
  walIndexHeader *pIndexHeader = NULL;

  while (size > 0) {
    // read header
    walIndexHeader *pHeader = (walIndexHeader*)buffer;
    if (pIndexHeader == NULL) {
      pIndexHeader = pHeader;
    }
    assert(pHeader->meta.type == WAL_INDEX_HEADER);
    buffer += sizeof(walIndexHeaderMeta) + pHeader->meta.walNameSize;
    memset(name, 0, sizeof(name));
    memcpy(name, pHeader->walName, pHeader->meta.walNameSize);

    beginBuildWalIndex(name);

    size -= sizeof(walIndexHeaderMeta) + pHeader->meta.walNameSize;

    int64_t total = pHeader->meta.totalSize;
    uint64_t walVersion = pHeader->meta.version;

    tsSdbIndexVersion = walVersion;

    int64_t fd = tfOpen(name, O_RDONLY);
    if (!tfValid(fd)) {
      sdbError("file:%s, failed to open for build index since %s", name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    // read table header
    while (total > 0) {
      walIndexTableHeader *pTableHeader = (walIndexTableHeader*)buffer;
      assert(pTableHeader->type == WAL_INDEX_TABLE_HEADER);

      ESdbTable tableId = pTableHeader->tableId;
      int64_t tableSize = pTableHeader->tableSize;
      buffer += sizeof(walIndexTableHeader);

      total -= sizeof(walIndexTableHeader);

      int64_t readBytes = 0;
      while (readBytes < tableSize) {
        walIndex* pIndex = (walIndex*)buffer;
        buffer += sizeof(walIndex) + pIndex->keyLen + pIndex->parentKeyLen;
        readBytes += sizeof(walIndex) + pIndex->keyLen  + pIndex->parentKeyLen;

        addExistIndex(tableId, pIndex);
      }
      assert(readBytes == tableSize);

      total -= readBytes;
    }
    assert(total == 0);

    size -= pHeader->meta.totalSize;
  }

_err:
  if (code != TSDB_CODE_SUCCESS) {
    tfree(*pSave);
    pIndexHeader = NULL;
  }
  return pIndexHeader;
}

static void saveIndex() {
  char* save = NULL;
  char    name[TSDB_FILENAME_LEN] = {0};
  sprintf(name, "%s/wal/index", tsMnodeDir);
  sdbInfo("begin save index to file:%s success", name);
  remove(name);
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t tfd = tfOpenM(name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (!tfValid(tfd)) {
    sdbError("file:%s, failed to open for build index since %s", name, strerror(errno));
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }  

  walIndexFileInfo* pFileInfo = tsWalFileInfo;
  while (pFileInfo) {
    assert(sizeof(walIndexHeaderMeta) == sizeof(walIndexContentType) + sizeof(int64_t)*2 + sizeof(uint64_t) + sizeof(int32_t) );
    int64_t headerSize = sizeof(walIndexHeaderMeta) + strlen(pFileInfo->name);

    int64_t indexTotal = pFileInfo->total;
    int64_t tableHeaderTotal = SDB_TABLE_MAX*(sizeof(walIndexTableHeader));
    int64_t total = headerSize + indexTotal + tableHeaderTotal;

    char *buffer = calloc(1, total);
    if (buffer == NULL) {
      code = -1;
      goto _err;
    }
    save = buffer;

    // save header
    walIndexHeaderMeta *pIndexHeader = (walIndexHeaderMeta*)buffer;
    *pIndexHeader = (walIndexHeaderMeta) {
      .type         = WAL_INDEX_HEADER,
      .totalSize    = indexTotal + tableHeaderTotal,
      .version      = pFileInfo->version,
      .offset       = pFileInfo->offset,
      .walNameSize  = strlen(pFileInfo->name),
    };
    buffer += sizeof(walIndexHeaderMeta);
    memcpy(buffer, pFileInfo->name, strlen(pFileInfo->name)); buffer += strlen(pFileInfo->name);

    // build table info
    int i = 0;
    for (i = 0; i < SDB_TABLE_MAX; ++i) {
      // build table header
      walIndexTableHeader *pTableHeader = (walIndexTableHeader*)buffer;
      *pTableHeader = (walIndexTableHeader) {
        .type = WAL_INDEX_TABLE_HEADER,
        .tableId = i,
        .tableSize = pFileInfo->tableSize[i],
      };
      buffer += sizeof(walIndexTableHeader);

      char* pTableBegin = buffer;

      // build index item array
      walIndexItem* pItem = pFileInfo->head[i];
      while (pItem) {
        memcpy(buffer, pItem->pIndex, sizeof(walIndex) + pItem->pIndex->keyLen + pItem->pIndex->parentKeyLen);
        buffer += sizeof(walIndex) + pItem->pIndex->keyLen + pItem->pIndex->parentKeyLen;
        pItem = pItem->next;
      }

      assert((buffer - pTableBegin) == pFileInfo->tableSize[i]);  
    }
    assert(buffer - save == total);

    tfWrite(tfd, save, total);
    tfFsync(tfd);
    tfree(save);

    pFileInfo = pFileInfo->next;
  }
  
_err:
  if (tfValid(tfd)) tfClose(tfd);
  tfree(save);

  sdbInfo("save index to file:%s %s", name, code == TSDB_CODE_SUCCESS ? "success" : "fail");
}

void mnodeSdbBuildWalIndex(void* handle) {
  tsWalFileInfo = NULL;

  // init table index array
  int i = 0;
  for (i = 0; i < SDB_TABLE_MAX; ++i) {
    ESdbKey keyType = sdbGetKeyTypeByTableId(i);
    _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
    if (keyType == SDB_KEY_STRING || keyType == SDB_KEY_VAR_STRING) {
      hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
    }
    tsIndexItemTable[i] = taosHashInit(1024, hashFp, true, HASH_ENTRY_LOCK);
  }

  // get wal file list
  SWalFileItem* pFileList = walRestoreFileList(handle);
  SWalFileItem* pFileItem, *next, *current;
  if (pFileList == NULL) {
    goto _err;
  }

  // read index file if any
  char* pSave = NULL;
  readIndex(&pSave);  

  // build wal index increment
  walIndexFileInfo* pFileInfo = tsWalFileInfo;
  pFileItem = pFileList;
  while (pFileItem) {
    current = pFileItem;
    pFileItem = pFileItem->next;

    // find index if any
    int64_t offset = 0;
    while (pFileInfo) {
      if (strcmp(pFileInfo->name, current->walName) == 0) break;
      pFileInfo = pFileInfo->next;
    }
    
    if (pFileInfo) {
      offset = pFileInfo->offset;
    } else {
      // create new file info
      beginBuildWalIndex(current->walName);
    }
    // restore from index
    int32_t code = walRestoreFrom(handle, NULL, current->walName, offset, buildIndex);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }    
  }

  // ok, let's build index
  saveIndex();

_err:
  pFileItem = pFileList;
  while (pFileItem) {
    next = pFileItem->next;
    free(pFileItem->walName);
    free(pFileItem);
    pFileItem = next;
  }

  pFileInfo = tsWalFileInfo;
  while (pFileInfo) {    
    for (i = 0; i < SDB_TABLE_MAX; ++i) {
      walIndexItem* pItem = pFileInfo->head[i];
      while (pItem) {
        walIndexItem* pNext = pItem->next;
        if (pItem->needFreeIndex) free(pItem->pIndex);
        free(pItem);
        pItem = pNext;
      }              
    }

    walIndexFileInfo* pNextFileInfo = pFileInfo->next;
    free(pFileInfo);
    pFileInfo = pNextFileInfo;         
  }

  for (i = 0; i < SDB_TABLE_MAX; ++i) {      
    taosHashCleanup(tsIndexItemTable[i]);
  }

  tsWalFileInfo = NULL;  
}

static int32_t sdbRestoreFromWalFiles(SWalFileItem* pFileList, twalh handle, FWalWrite writeFp) {
  SWalFileItem* current = pFileList;
  int code = TSDB_CODE_SUCCESS;

  while (current) {
    // this wal file not in the wal index
    current->hasProcess = true;
    code = walRestoreWalFile(handle, NULL, 0, writeFp, current->walName);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    current = current->next;
  }

  return code;
}

int32_t sdbRestoreFromIndex(twalh handle, FWalIndexReader fpReader, FWalWrite writeFp) {
  char    name[TSDB_FILENAME_LEN] = {0};
  sprintf(name, "%s/wal/index", tsMnodeDir);
  int code = TSDB_CODE_SUCCESS;
  SWalFileItem* pFileList = walRestoreFileList(handle);
  SWalFileItem* pFileItem, *next, *current;

  if (pFileList == NULL) {
    goto _err;
  }

  int64_t tfd = tfOpen(name, O_RDONLY);
  if (!tfValid(tfd)) {
    sdbInfo("file:%s, failed to open index since %s, restore from wal", name, strerror(errno));
    // no index, restore from wal files
    return sdbRestoreFromWalFiles(pFileList, handle, writeFp);
  }

  int64_t size = tfLseek(tfd, 0, SEEK_END);
  if (size < 0) {
    sdbInfo("file:%s, failed to open index since %s, restore from wal", name, strerror(errno));
    // no index, restore from wal files
    return sdbRestoreFromWalFiles(pFileList, handle, writeFp);
  }

  char *buffer = calloc(1, size);
  if (buffer == NULL) {
    code = TSDB_CODE_COM_OUT_OF_MEMORY;
    goto _err;
  }
  tfLseek(tfd, 0, SEEK_SET);
  if (tfRead(tfd, buffer, size) != size) {
    sdbError("file:%s, failed to read since %s", name, strerror(errno));
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  char* save = buffer;
  current = pFileList;
  while (size > 0) {
    // read header
    walIndexHeader *pHeader = (walIndexHeader*)buffer;
    assert(pHeader->meta.type == WAL_INDEX_HEADER);
    buffer += sizeof(walIndexHeaderMeta) + pHeader->meta.walNameSize;
    memset(name, 0, sizeof(name));
    memcpy(name, pHeader->walName, pHeader->meta.walNameSize);
    size -= sizeof(walIndexHeaderMeta) + pHeader->meta.walNameSize;

    // mark has processed from wal file list
    while (current && strcmp(current->walName, name) != 0) {
      // this wal file not in the wal index
      current->hasProcess = true;
      code = walRestoreWalFile(handle, NULL, 0, writeFp, current->walName);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }
      current = current->next;
    }
    if (!current) {
      break;
    }
    assert(strcmp(current->walName, name) == 0);

    int64_t total = pHeader->meta.totalSize;
    uint64_t walVersion = pHeader->meta.version;

    int64_t fd = tfOpen(name, O_RDONLY);
    if (!tfValid(fd)) {
      sdbError("file:%s, failed to open for build index since %s", name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    // read table header
    while (total > 0) {
      walIndexTableHeader *pTableHeader = (walIndexTableHeader*)buffer;
      assert(pTableHeader->type == WAL_INDEX_TABLE_HEADER);

      ESdbTable tableId = pTableHeader->tableId;
      int64_t tableSize = pTableHeader->tableSize;
      buffer += sizeof(walIndexTableHeader);

      total -= sizeof(walIndexTableHeader);

      int64_t readBytes = 0;
      while (readBytes < tableSize) {
        walIndex* pIndex = (walIndex*)buffer;
        buffer += sizeof(walIndex) + pIndex->keyLen + pIndex->parentKeyLen;
        readBytes += sizeof(walIndex) + pIndex->keyLen + pIndex->parentKeyLen;

        fpReader(fd, name, tableId, walVersion, pIndex);
      }
      assert(readBytes == tableSize);

      total -= readBytes;
    }
    assert(total == 0);

    code = walRestoreFrom(handle, NULL, name, pHeader->meta.offset, writeFp);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    size -= pHeader->meta.totalSize;
  }

_err:
  pFileItem = pFileList;
  while (pFileItem) {
    next = pFileItem->next;
    free(pFileItem->walName);
    free(pFileItem);
    pFileItem = next;
  }

  tfClose(tfd);
  tfree(save);
  return code;
}
