/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "os.h"
#include "shell.h"
#include "tconfig.h"
#include "tnettest.h"

pthread_t pid;
static tsem_t cancelSem;

void shellQueryInterruptHandler(int32_t signum, void *sigInfo, void *context) {
  tsem_post(&cancelSem);
}

void *cancelHandler(void *arg) {
  while(1) {
    if (tsem_wait(&cancelSem) != 0) {
      taosMsleep(10);
      continue;
    }

#ifdef LINUX
    int64_t rid = atomic_val_compare_exchange_64(&result, result, 0);
    SSqlObj* pSql = taosAcquireRef(tscObjRef, rid);
    taos_stop_query(pSql);
    taosReleaseRef(tscObjRef, rid);
#else
    printf("\nReceive ctrl+c or other signal, quit shell.\n");
    exit(0);
#endif
  }
  
  return NULL;
}

int checkVersion() {
  if (sizeof(int8_t) != 1) {
    printf("taos int8 size is %d(!= 1)", (int)sizeof(int8_t));
    return 0;
  }
  if (sizeof(int16_t) != 2) {
    printf("taos int16 size is %d(!= 2)", (int)sizeof(int16_t));
    return 0;
  }
  if (sizeof(int32_t) != 4) {
    printf("taos int32 size is %d(!= 4)", (int)sizeof(int32_t));
    return 0;
  }
  if (sizeof(int64_t) != 8) {
    printf("taos int64 size is %d(!= 8)", (int)sizeof(int64_t));
    return 0;
  }
  return 1;
}

// Global configurations
SShellArguments args = {
  .host = NULL,
  .password = NULL,
  .user = NULL,
  .database = NULL,
  .timezone = NULL,
  .is_raw_time = false,
  .is_use_passwd = false,
  .dump_config = false,
  .file = "\0",
  .dir = "\0",
  .threadNum = 5,
  .commands = NULL,
  .pktLen = 1000,
  .netTestRole = NULL
};

#define STR_TO_NUM(s) (((s) >= '0' && (s) <= '9') ? ((s) - '0') : (10 + ((s) - 'a')))
extern int tsDecompressStringImp(const char *const input, int compressedSize, char *const output, int outputSize);
extern int tsCompressTimestampImp(const char *const input, const int nelements, char *const output);
extern int tsCompressStringImp(const char *const input, int inputSize, char *const output, int outputSize);
extern int tsDecompressTimestampImp(const char *const input, const int nelements, char *const output);

/*
 * Main function.
 */
#define NO_COMPRESSION 0
#define ONE_STAGE_COMP 1
#define TWO_STAGE_COMP 2

static FORCE_INLINE int tsCompressTimestamp1(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                        char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressTimestampImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressTimestamp1(const char *const input, int compressedSize, const int nelements, char *const output,
                          int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressTimestampImp(buffer, nelements, output);
  } else {
    assert(0);
    return -1;
  }
}



#include "os.h"
                        
#include "qTsbuf.h"
#include "taos.h"
#include "tsdb.h"
#include "ttoken.h"
#include "tutil.h"
                        
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

#define EXPECT_EQ(a, b) assert((a) == (b))
#define EXPECT_STREQ(a, b) assert(strcmp(a, b) == 0)
#define EXPECT_TRUE(a) assert(a)
/**
 *
 * @param num  total number
 * @param step gap between two consecutive ts
 * @return
 */
int64_t* createTsList(int32_t num, int64_t start, int32_t step) {
  int64_t* pList = (int64_t*)malloc(num * sizeof(int64_t));

  for (int64_t i = 0; i < num; ++i) {
    pList[i] = start + i * step;
  }

  return pList;
}

// simple test
void simpleTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  // write 10 ts points
  int32_t num = 10;
  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;
  t.i64 = 1;

  int64_t* list = createTsList(10, 10000000, 30);
  tsBufAppend(pTSBuf, 0, &t, (const char*)list, num * sizeof(int64_t));
  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);

  EXPECT_EQ(pTSBuf->tsData.len, sizeof(int64_t) * num);
  EXPECT_EQ(tVariantCompare(&pTSBuf->block.tag, &t), 0);
  EXPECT_EQ(pTSBuf->numOfGroups, 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);

  free(list);
}

// one large list of ts, the ts list need to be split into several small blocks
void largeTSTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  // write 10 ts points
  int32_t num = 1000000;
  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;
  t.i64 = 1;

  int64_t* list = createTsList(num, 10000000, 30);
  tsBufAppend(pTSBuf, 0, &t, (const char*)list, num * sizeof(int64_t));

  // the data has been flush to disk, no data in cache
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(tVariantCompare(&pTSBuf->block.tag, &t), 0);
  EXPECT_EQ(pTSBuf->numOfGroups, 1);
  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);
  free(list);
}

void multiTagsTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t num = 10000;
  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;

  int64_t start = 10000000;
  int32_t numOfTags = 50;
  int32_t step = 30;

  for (int32_t i = 0; i < numOfTags; ++i) {
    int64_t* list = createTsList(num, start, step);
    t.i64 = i;

    tsBufAppend(pTSBuf, 0, &t, (const char*)list, num * sizeof(int64_t));
    free(list);

    start += step * num;
  }

  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);
  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));

  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);
  EXPECT_EQ(pTSBuf->numOfGroups, 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);
}

void multiVnodeTagsTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t num = 10000;
  int64_t start = 10000000;
  int32_t numOfTags = 50;
  int32_t step = 30;

  // 2000 vnodes
  for (int32_t j = 0; j < 20; ++j) {
    // vnodeId:0
    start = 10000000;
    tVariant t = {0};
    t.nType = TSDB_DATA_TYPE_BIGINT;

    for (int32_t i = 0; i < numOfTags; ++i) {
      int64_t* list = createTsList(num, start, step);
      t.i64 = i;

      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      free(list);

      start += step * num;
    }

    EXPECT_EQ(pTSBuf->numOfGroups, j + 1);
  }

  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);
  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));
  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));

  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  tsBufDestroy(pTSBuf);
}

void loadDataTest() {
  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t num = 10000;
  int64_t oldStart = 10000000;
  int32_t numOfTags = 50;
  int32_t step = 30;
  int32_t numOfVnode = 200;

  // 10000 vnodes
  for (int32_t j = 0; j < numOfVnode; ++j) {
    // vnodeId:0
    int64_t start = 10000000;
    tVariant t = {0};
    t.nType = TSDB_DATA_TYPE_BIGINT;

    for (int32_t i = 0; i < numOfTags; ++i) {
      int64_t* list = createTsList(num, start, step);
      t.i64 = i;

      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      printf("%d - %" PRIu64 "\n", i, list[0]);

      free(list);
      start += step * num;
    }

    EXPECT_EQ(pTSBuf->numOfGroups, j + 1);
  }

  EXPECT_EQ(pTSBuf->tsOrder, TSDB_ORDER_ASC);

  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));
  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  EXPECT_EQ(pTSBuf->tsData.len, num * sizeof(int64_t));

  EXPECT_EQ(pTSBuf->block.tag.i64, numOfTags - 1);

  tsBufFlush(pTSBuf);
  EXPECT_EQ(pTSBuf->tsData.len, 0);
  EXPECT_EQ(pTSBuf->block.numOfElem, num);

  // create from exists file
  STSBuf* pNewBuf = tsBufCreateFromFile(pTSBuf->path, false);
  EXPECT_EQ(pNewBuf->tsOrder, pTSBuf->tsOrder);
  EXPECT_EQ(pNewBuf->numOfGroups, numOfVnode);
  EXPECT_EQ(pNewBuf->fileSize, pTSBuf->fileSize);

  EXPECT_EQ(pNewBuf->pData[0].info.offset, pTSBuf->pData[0].info.offset);
  EXPECT_EQ(pNewBuf->pData[0].info.numOfBlocks, pTSBuf->pData[0].info.numOfBlocks);
  EXPECT_EQ(pNewBuf->pData[0].info.compLen, pTSBuf->pData[0].info.compLen);

  EXPECT_STREQ(pNewBuf->path, pTSBuf->path);

  tsBufResetPos(pNewBuf);

  int64_t s = taosGetTimestampUs();
  printf("start:%" PRIu64 "\n", s);

  int32_t x = 0;
  while (tsBufNextPos(pNewBuf)) {
    STSElem elem = tsBufGetElem(pNewBuf);
    if (++x == 100000000) {
      break;
    }

    //    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.vnode, elem.tag, elem.ts);
  }

  int64_t e = taosGetTimestampUs();
  printf("end:%" PRIu64 ", elapsed:%" PRIu64 ", total obj:%d\n", e, e - s, x);
  tsBufDestroy(pTSBuf);
  tsBufDestroy(pNewBuf);
}

void randomIncTsTest() {}

void TSTraverse() {
  // 10000 vnodes
  int32_t num = 200000;
  int64_t oldStart = 10000000;
  int32_t numOfTags = 3;
  int32_t step = 30;
  int32_t numOfVnode = 2;

  STSBuf* pTSBuf = tsBufCreate(true, TSDB_ORDER_ASC);

  for (int32_t j = 0; j < numOfVnode; ++j) {
    // vnodeId:0
    int64_t start = 10000000;
    tVariant t = {0};
    t.nType = TSDB_DATA_TYPE_BIGINT;

    for (int32_t i = 0; i < numOfTags; ++i) {
      int64_t* list = createTsList(num, start, step);
      t.i64 = i;

      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      printf("%d - %d - %" PRIu64 ", %" PRIu64 "\n", j, i, list[0], list[num - 1]);

      free(list);
      start += step * num;

      list = createTsList(num, start, step);
      tsBufAppend(pTSBuf, j, &t, (const char*)list, num * sizeof(int64_t));
      printf("%d - %d - %" PRIu64 ", %" PRIu64 "\n", j, i, list[0], list[num - 1]);
      free(list);

      start += step * num;
    }

    EXPECT_EQ(pTSBuf->numOfGroups, j + 1);
  }

  tsBufResetPos(pTSBuf);

  ////////////////////////////////////////////////////////////////////////////////////////
  // reverse traverse
  int64_t s = taosGetTimestampUs();
  printf("start:%" PRIu64 "\n", s);

  pTSBuf->cur.order = TSDB_ORDER_DESC;

  // complete reverse traverse
  int32_t x = 0;
  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    //    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.vnode, elem.tag, elem.ts);
  }

  // specify the data block with vnode and tags value
  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSDB_ORDER_DESC;

  int32_t startVnode = 1;
  int32_t startTag = 2;

  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;
  t.i64 = startTag;

  tsBufGetElemStartPos(pTSBuf, startVnode, &t);

  int32_t totalOutput = 10;
  while (1) {
    STSElem elem = tsBufGetElem(pTSBuf);
    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.id, elem.tag->i64, elem.ts);

    if (!tsBufNextPos(pTSBuf)) {
      break;
    }

    if (--totalOutput <= 0) {
      totalOutput = 10;

      startTag -= 1;
      t.i64 = startTag;
      tsBufGetElemStartPos(pTSBuf, startVnode, &t);

      if (startTag == 0) {
        startVnode -= 1;
        startTag = 3;
      }

      if (startVnode < 0) {
        break;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  // traverse
  pTSBuf->cur.order = TSDB_ORDER_ASC;
  tsBufResetPos(pTSBuf);

  // complete forwards traverse
  while (tsBufNextPos(pTSBuf)) {
    STSElem elem = tsBufGetElem(pTSBuf);
    //    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.vnode, elem.tag, elem.ts);
  }

  // specify the data block with vnode and tags value
  tsBufResetPos(pTSBuf);
  pTSBuf->cur.order = TSDB_ORDER_ASC;

  startVnode = 1;
  startTag = 2;
  t.i64 = startTag;

  tsBufGetElemStartPos(pTSBuf, startVnode, &t);

  totalOutput = 10;
  while (1) {
    STSElem elem = tsBufGetElem(pTSBuf);
    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.id, elem.tag->i64, elem.ts);

    if (!tsBufNextPos(pTSBuf)) {
      break;
    }

    if (--totalOutput <= 0) {
      totalOutput = 10;

      startTag -= 1;
      t.i64 = startTag;
      tsBufGetElemStartPos(pTSBuf, startVnode, &t);

      if (startTag < 0) {
        startVnode -= 1;
        startTag = 3;
      }

      if (startVnode < 0) {
        break;
      }
    }
  }

  tsBufDestroy(pTSBuf);
}

void performanceTest() {}

void emptyTagTest() {}

void invalidFileTest() {
  const char* cmd = "touch /tmp/test";

  // create empty file
  system(cmd);

  STSBuf* pNewBuf = tsBufCreateFromFile("/tmp/test", true);
  EXPECT_TRUE(pNewBuf == NULL);
  tsBufDestroy(pNewBuf);

  pNewBuf = tsBufCreateFromFile("/tmp/911", true);
  EXPECT_TRUE(pNewBuf == NULL);

  tsBufDestroy(pNewBuf);
}

void mergeDiffVnodeBufferTest() {
  STSBuf* pTSBuf1 = tsBufCreate(true, TSDB_ORDER_ASC);
  STSBuf* pTSBuf2 = tsBufCreate(true, TSDB_ORDER_ASC);

  int32_t step = 30;
  int32_t num = 1000;
  int32_t numOfTags = 10;

  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;

  // vnodeId:0
  int64_t start = 10000000;
  for (int32_t i = 0; i < numOfTags; ++i) {
    int64_t* list = createTsList(num, start, step);
    t.i64 = i;

    tsBufAppend(pTSBuf1, 1, &t, (const char*)list, num * sizeof(int64_t));
    tsBufAppend(pTSBuf2, 9, &t, (const char*)list, num * sizeof(int64_t));

    free(list);

    start += step * num;
  }

  tsBufFlush(pTSBuf2);

  tsBufMerge(pTSBuf1, pTSBuf2);
  EXPECT_EQ(pTSBuf1->numOfGroups, 2);
  EXPECT_EQ(pTSBuf1->numOfTotal, numOfTags * 2 * num);

  tsBufDisplay(pTSBuf1);

  tsBufDestroy(pTSBuf2);
  tsBufDestroy(pTSBuf1);
}

void mergeIdenticalVnodeBufferTest() {
  STSBuf* pTSBuf1 = tsBufCreate(true, TSDB_ORDER_ASC);
  STSBuf* pTSBuf2 = tsBufCreate(true, TSDB_ORDER_ASC);

  tVariant t = {0};
  t.nType = TSDB_DATA_TYPE_BIGINT;

  int32_t step = 30;
  int32_t num = 1000;
  int32_t numOfTags = 10;

  // vnodeId:0
  int64_t start = 10000000;
  for (int32_t i = 0; i < numOfTags; ++i) {
    int64_t* list = createTsList(num, start, step);
    t.i64 = i;

    tsBufAppend(pTSBuf1, 12, &t, (const char*)list, num * sizeof(int64_t));
    free(list);

    start += step * num;
  }

  for (int32_t i = numOfTags; i < numOfTags * 2; ++i) {
    int64_t* list = createTsList(num, start, step);

    t.i64 = i;
    tsBufAppend(pTSBuf2, 77, &t, (const char*)list, num * sizeof(int64_t));
    free(list);

    start += step * num;
  }

  tsBufFlush(pTSBuf2);

  tsBufMerge(pTSBuf1, pTSBuf2);
  EXPECT_EQ(pTSBuf1->numOfGroups, 2);
  EXPECT_EQ(pTSBuf1->numOfTotal, numOfTags * 2 * num);

  tsBufResetPos(pTSBuf1);

  int32_t count = 0;
  while (tsBufNextPos(pTSBuf1)) {
    STSElem elem = tsBufGetElem(pTSBuf1);

    if (count++ < numOfTags * num) {
      EXPECT_EQ(elem.id, 12);
    } else {
      EXPECT_EQ(elem.id, 77);
    }

    printf("%d-%" PRIu64 "-%" PRIu64 "\n", elem.id, elem.tag->i64, elem.ts);
  }

  tsBufDestroy(pTSBuf1);
  tsBufDestroy(pTSBuf2);
}


int main(int argc, char* argv[]) {
  /*setlocale(LC_ALL, "en_US.UTF-8"); */
#if 0
    simpleTest();
    largeTSTest();
    multiTagsTest();
    multiVnodeTagsTest();
    loadDataTest();
    invalidFileTest();
  //    randomIncTsTest();
    TSTraverse();
    mergeDiffVnodeBufferTest();
    mergeIdenticalVnodeBufferTest();

    return 0;
#endif

#if 1
  int64_t *input0 = NULL;
  char *output0 = malloc(104857600);
  char *tmp0 = malloc(1048576);
  char *tmp1 = malloc(1048576);

  input0 = calloc(10485760, TSDB_KEYSIZE);

  int nnn = TSDB_KEYSIZE * 131072;
  int i;

  srand(time(NULL));
  int64_t ts0 = 123231231322;
  
  for (i=0;i<131072;++i) {
    input0[i] = 123231231322 + rand() % 100;
  }
  
    int tssize =
        tsCompressTimestamp1((char *)input0, nnn, nnn/TSDB_KEYSIZE, output0, 1048576,
        TWO_STAGE_COMP, tmp0, 1048576);

    printf("try, encodesize:%d\n", tssize);

//            TWO_STAGE_COMP, tmp0, 104857600);
    int ds = tsDecompressStringImp(output0, tssize, tmp1, 1048576);
    
    if (ds < 0) {
      printf("de failed, size:%d\n", nnn);
      assert(0);
      return -1;
    }
    
    assert(ds + 1 >= tssize);
    printf("try %d success, encodesize:%d,decodesize:%d\n", nnn, tssize, ds+1);



  return 0;
#endif  

#if 0

  FILE* fd1=fopen("/tmp/block_data.txt", "r");
  char in[1024] = {0};
  char out[560000] = {0};
  char *f = malloc(10485761000);

  int i = 0, o = 0;
  while(fgets(in, sizeof(in), fd1) != NULL)
  {
    i = 0;
    while (in[i]) {
      if ((in[i] >= '0' && in[i] <= '9') || (in[i] >= 'a' && in[i] <= 'f')) {
        if ((in[i+1] >= '0' && in[i+1] <= '9') || (in[i+1] >= 'a' && in[i+1] <= 'f')) {
          out[o++] = STR_TO_NUM(in[i]) * 16 + STR_TO_NUM(in[i+1]);
          i += 2;
          
          continue;
        } else {
           printf("error data\n");
           exit(1);
        }
      } else {
        ++i;
      }
    }

  }
  
  printf("read out:%d\n", o);


//  int ds = tsDecompressStringImp(out, o, f, 1048576 * 1000);

  char *tmpbuf = calloc(1048576, 1);
  char *tmpbuf2 = calloc(1048576, 1);
  //qDump(out, 204441);  
  int ds = tsDecompressTimestamp1(out, 204441, 131682, tmpbuf, 1048576, TWO_STAGE_COMP, tmpbuf2, 1048576);  
  assert((ds / TSDB_KEYSIZE == 131682) && (1048576 >= ds));
  tfree(tmpbuf);
  tfree(tmpbuf2);




  if (ds < 0) {
    printf("failed\n");
    return -1;
  }

  printf("success,ds:%d\n", ds);
  
  return 0;
#endif

  if (!checkVersion()) {
    exit(EXIT_FAILURE);
  }

  shellParseArgument(argc, argv, &args);

  if (args.dump_config) {
    taosInitGlobalCfg();
    taosReadGlobalLogCfg();

    if (!taosReadGlobalCfg()) {
      printf("TDengine read global config failed");
      exit(EXIT_FAILURE);
    }

    taosDumpGlobalCfg();
    exit(0);
  }

  if (args.netTestRole && args.netTestRole[0] != 0) {
    if (taos_init()) {
      printf("Failed to init taos");
      exit(EXIT_FAILURE);
    }
    taosNetTest(args.netTestRole, args.host, args.port, args.pktLen);
    exit(0);
  }

  /* Initialize the shell */
  TAOS* con = shellInit(&args);
  if (con == NULL) {
    exit(EXIT_FAILURE);
  }

  if (tsem_init(&cancelSem, 0, 0) != 0) {
    printf("failed to create cancel semphore\n");
    exit(EXIT_FAILURE);
  }

  pthread_t spid;
  pthread_create(&spid, NULL, cancelHandler, NULL);

  /* Interrupt handler. */
  taosSetSignal(SIGTERM, shellQueryInterruptHandler);
  taosSetSignal(SIGINT, shellQueryInterruptHandler);
  taosSetSignal(SIGHUP, shellQueryInterruptHandler);
  taosSetSignal(SIGABRT, shellQueryInterruptHandler);

  /* Get grant information */
  shellGetGrantInfo(con);

  /* Loop to query the input. */
  while (1) {
    pthread_create(&pid, NULL, shellLoopQuery, con);
    pthread_join(pid, NULL);
  }
}
