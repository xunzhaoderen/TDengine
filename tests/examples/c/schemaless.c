#include "os.h"
#include "taos.h"
#include "taoserror.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <string.h>


#define MAX_THREAD_LINE_BATCHES 1024

void printThreadId(pthread_t id, char* buf)
{
  size_t i;
  for (i = sizeof(i); i; --i)
    sprintf(buf + strlen(buf), "%02x", *(((unsigned char*) &id) + i - 1));
}

//static int64_t getTimeInUs() {
//  struct timeval systemTime;
//  gettimeofday(&systemTime, NULL);
//  return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
//}

typedef struct {
  int numThreads;
  int numSuperTables;
  int numChildTables;
  int numRowsPerChildTable;
  int numFields;
  int maxLinesPerBatch;
  char* lineTemplate;
} SRunPara;

typedef struct {
  char** lines;
  int numLines;
} SThreadLinesBatch;

typedef struct  {
  TAOS* taos;
  int   threadID;
  uint64_t  start_table_from;
  uint64_t  end_table_to;
  int64_t   ntables;
  int64_t   start_time;
  int64_t   timeStampStep;

  uint64_t  totalInsertRows;
  
  int   numBatches;
  SThreadLinesBatch batches[MAX_THREAD_LINE_BATCHES];
  int64_t costTime;
} SThreadInsertArgs;



static SRunPara g_args = {0,};


#if 0
static void* insertLines_old(void* args) {
  SThreadInsertArgs* insertArgs = (SThreadInsertArgs*) args;
  (void)taos_select_db(insertArgs->taos, "db");
  char tidBuf[32] = {0};
  printThreadId(pthread_self(), tidBuf);

  for (int x=0; x < 1; x++) {  
    for (int i = 0; i < insertArgs->numBatches; ++i) {
      SThreadLinesBatch* batch = insertArgs->batches + i;
      //printf("%s, thread: 0x%s\n", "begin taos_insert_lines", tidBuf);
      int64_t begin = getTimeInUs();
      int32_t code = taos_insert_lines(insertArgs->taos, batch->lines, batch->numLines);
      int64_t end = getTimeInUs();
      insertArgs->costTime += end - begin;
      if (code != TSDB_CODE_SUCCESS) {
        printf("code: %d, %s. time used:%"PRId64", thread: 0x%s\n", code, tstrerror(code), end - begin, tidBuf);
      }
    }
  }
  return NULL;
}

static void* insertLines(void* args) {
  SThreadInsertArgs* insertArgs = (SThreadInsertArgs*) args;
  (void)taos_select_db(insertArgs->taos, "db");
  char tidBuf[32] = {0};
  printThreadId(pthread_self(), tidBuf);

  for (int x=0; x < 1; x++) {  
    for (int i = 0; i < insertArgs->numBatches; ++i) {
      SThreadLinesBatch* batch = insertArgs->batches + i;
      //printf("%s, thread: 0x%s\n", "begin taos_insert_lines", tidBuf);
      int64_t begin = getTimeInUs();
      int32_t code = taos_insert_lines(insertArgs->taos, batch->lines, batch->numLines);
      int64_t end = getTimeInUs();
      insertArgs->costTime += end - begin;
      if (code != TSDB_CODE_SUCCESS) {
        printf("code: %d, %s. time used:%"PRId64", thread: 0x%s\n", code, tstrerror(code), end - begin, tidBuf);
      }
    }
  }
  return NULL;
}
#endif

#if 1
static void* syncWrite(void *sarg) {
    SThreadInsertArgs *pThreadInfo = (SThreadInsertArgs *)sarg;
    
    int64_t startTimestamp = 1577808000000;
    int64_t timestampStep  = 1;

    int  lenOfOneRows;
    lenOfOneRows = sizeof(g_args.lineTemplate)+128;  // 128 for timestamp    
    char* lineBuf = (char*)calloc(g_args.maxLinesPerBatch * lenOfOneRows, sizeof(char));
    if (NULL == lineBuf) {
      return NULL;
    }

    char** lineArry = (char**)calloc(g_args.maxLinesPerBatch, sizeof(char*));
    if (NULL == lineArry) {
      return NULL;
    }

    for (int x = 0; x < g_args.maxLinesPerBatch; x++) {
      lineArry[x] = lineBuf + x * lenOfOneRows;
    }

    for (uint64_t tableSeq = pThreadInfo->start_table_from; tableSeq <= pThreadInfo->end_table_to; tableSeq ++) {

        for (int i = 0; i < g_args.numRowsPerChildTable;) {
            int32_t k = 0;
            for (k = 0; k < g_args.maxLinesPerBatch;) {
                snprintf(lineArry[k], lenOfOneRows, g_args.lineTemplate, 0, tableSeq, startTimestamp + i * timestampStep);
                k++;
                i++;
                if (i >= g_args.numRowsPerChildTable) {
                    break;
                }
            }

            pThreadInfo->totalInsertRows += k;

            //startTs = taosGetTimestampUs();

            int32_t code = taos_insert_lines(pThreadInfo->taos, lineArry, k);
            if (code != TSDB_CODE_SUCCESS) {
              printf("taos_insert_lines() faile, code: %d, %s\n", code, tstrerror(code));
            }

            //endTs = taosGetTimestampUs();
            //uint64_t delay = endTs - startTs;

            if (i >= g_args.numRowsPerChildTable) break;
        }   
    }

    return NULL;
}
#endif


int32_t getLineTemplate_old(char* lineTemplate, int templateLen, int numFields) {
  if (numFields <= 4) {
    char* sample = "sta%d,t0=%di32,t1=\"binaryTagValue\" c0=2147483647i32,c1=2147483647i32,c2=2147483647i32,c3=2147483647i32 %lldms";
    snprintf(lineTemplate, templateLen, "%s", sample);
    return 0;
  }

  if (numFields <= 13) {
     char* sample = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32,t4=9223372036854775807i64,t9=11.12345f32,t10=22.123456789f64,t11=\"binaryTagValue\",t12=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=254u8,c6=32770u16,c7=2147483699u32,c8=9223372036854775899u64,c9=11.12345f32,c10=22.123456789f64,c11=\"binaryValue\",c12=L\"ncharValue\" %lldms";
     snprintf(lineTemplate, templateLen, "%s", sample);
     return 0;
  }

  char* lineFormatTable = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32 ";
  snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "%s", lineFormatTable);

  int offset[] = {numFields*2/5, numFields*4/5, numFields};

  for (int i = 0; i < offset[0]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=%di32,", i, i);
  }

  for (int i=offset[0]; i < offset[1]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=%d.43f64,", i, i);
  }

  for (int i = offset[1]; i < offset[2]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=\"%d\",", i, i);
  }
  char* lineFormatTs = " %lldms";
  snprintf(lineTemplate+strlen(lineTemplate)-1, templateLen-strlen(lineTemplate)+1, "%s", lineFormatTs);

  return 0;
}

int32_t getLineTemplate() {
  int   templateLen  = 65535;
  char* lineTemplate = g_args.lineTemplate;
  int   numFields    = g_args.numFields;

  if (numFields <= 4) {
    char* sample = "sta%d,t0=%di32,t1=\"binaryTagValue\" c0=2147483647i32,c1=2147483647i32,c2=2147483647i32,c3=2147483647i32 %lldms";
    snprintf(lineTemplate, templateLen, "%s", sample);
    return 0;
  }

  if (numFields <= 13) {
     char* sample = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32,t4=9223372036854775807i64,t9=11.12345f32,t10=22.123456789f64,t11=\"binaryTagValue\",t12=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=254u8,c6=32770u16,c7=2147483699u32,c8=9223372036854775899u64,c9=11.12345f32,c10=22.123456789f64,c11=\"binaryValue\",c12=L\"ncharValue\" %lldms";
     snprintf(lineTemplate, templateLen, "%s", sample);
     return 0;
  }

  char* lineFormatTable = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32 ";
  snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "%s", lineFormatTable);

  int offset[] = {numFields*2/5, numFields*4/5, numFields};

  for (int i = 0; i < offset[0]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=%di32,", i, i);
  }

  for (int i=offset[0]; i < offset[1]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=%d.43f64,", i, i);
  }

  for (int i = offset[1]; i < offset[2]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=\"%d\",", i, i);
  }
  char* lineFormatTs = " %lldms";
  snprintf(lineTemplate+strlen(lineTemplate)-1, templateLen-strlen(lineTemplate)+1, "%s", lineFormatTs);

  return 0;
}

#if 1
static int startMultiThreadInsertLine() {
    int      threads   = g_args.numThreads;
    int64_t  ntables   = g_args.numChildTables;
    uint64_t tableFrom = 0;
    
    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    SThreadInsertArgs *infos = calloc(1, threads * sizeof(SThreadInsertArgs));

    if ((NULL == pids) || (NULL == infos)) {
        exit(-1);
    }

    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = ntables;
        a = 1;
    }

    int64_t b = 0;
    b = ntables % threads;


    int64_t st = taosGetTimestampUs();

    for (int64_t i = 0; i < threads; i++) {
        SThreadInsertArgs *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 6030);
        if (pThreadInfo->taos == NULL) {
            printf("Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
            free(pids);
            free(infos);
            return -1;
        }
        (void)taos_select_db(pThreadInfo->taos, "db");

        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i<b?a+1:a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pthread_create(pids + i, NULL, syncWrite, pThreadInfo);
    }

    int  totalRows = 0;
    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int i = 0; i < threads; i++) {
        SThreadInsertArgs *pThreadInfo = infos + i;
        totalRows += pThreadInfo->totalInsertRows;
        taos_close(pThreadInfo->taos);
    }

    int64_t end = taosGetTimestampUs();
    
    printf("TOTAL LINES: %d\n", totalRows);
    printf("THREADS: %d\n", g_args.numThreads);
    printf("TIME: %d(ms)\n", (int)(end-st)/1000);
    double throughput = (double)(totalRows)/((double)(end-st) / 1000000);
    printf("THROUGHPUT:%d/s\n", (int)throughput);

    free(pids);
    free(infos);

    return 0;
}
#endif

int main(int argc, char* argv[]) {

  memset(&g_args, 0, sizeof(SRunPara));

  g_args.numThreads           = 10;
  g_args.numSuperTables       = 1;
  g_args.numChildTables       = 1000;
  g_args.numRowsPerChildTable = 100000;
  g_args.numFields            = 4;
  g_args.maxLinesPerBatch     = 65000;
  g_args.lineTemplate         = calloc(65536, sizeof(char));
  if (NULL == g_args.lineTemplate) {
    return -1;
  }

  int opt;
  while ((opt = getopt(argc, argv, "s:c:r:f:t:m:h")) != -1) {
    switch (opt) {
      case 's':
        g_args.numSuperTables = atoi(optarg);
        break;
      case 'c':
        g_args.numChildTables = atoi(optarg);
        break;
      case 'r':
        g_args.numRowsPerChildTable = atoi(optarg);
        break;
      case 'f':
        g_args.numFields = atoi(optarg);
        break;
      case 't':
        g_args.numThreads = atoi(optarg);
        break;
      case 'm':
        g_args.maxLinesPerBatch = atoi(optarg);
        break;
      case 'h':
        fprintf(stderr, "Usage: %s -s supertable -c childtable -r rows -f fields -t threads -m maxlines_per_batch\n",
                argv[0]);
        exit(0);
      default: /* '?' */
        fprintf(stderr, "Usage: %s -s supertable -c childtable -r rows -f fields -t threads -m maxlines_per_batch\n",
                argv[0]);
        exit(-1);
    }
  }

  TAOS_RES*   result;
  const char* host   = "127.0.0.1";
  const char* user   = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  result = taos_query(taos, "drop database if exists db;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database db update 1 precision 'us';");
  taos_free_result(result);
  usleep(100000);

  (void)taos_select_db(taos, "db");

  getLineTemplate();

  int64_t timestampStep = 1000;
  int64_t startTimestamp = 1577808000000 - timestampStep;
  
  printf("setup child tables...\n");
  {
    char** linesStb = calloc(g_args.numChildTables, sizeof(char*));
    for (int i = 0; i < g_args.numChildTables; i++) {
      char* lineStb = calloc(strlen(g_args.lineTemplate)+128, 1);
      snprintf(lineStb, strlen(g_args.lineTemplate)+128, g_args.lineTemplate, 0, i, startTimestamp - timestampStep);
      linesStb[i] = lineStb;
    }

    int32_t code = taos_insert_lines(taos, linesStb, g_args.numChildTables);
   
    for (int i = 0; i < g_args.numChildTables; ++i) {
      free(linesStb[i]);
    }
    free(linesStb);
    taos_close(taos);

    if (code != TSDB_CODE_SUCCESS) {
      printf("code: %d, %s.\n", code, tstrerror(code));
      return -1;
    }
  }

  printf("start create multi thread to insert rows for all child tables...\n");
  startMultiThreadInsertLine();
  
  free(g_args.lineTemplate);
  return 0;
}
