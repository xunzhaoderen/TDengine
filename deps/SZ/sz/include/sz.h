/**
 *  @file sz.h
 *  @author Sheng Di
 *  @date April, 2015
 *  @brief Header file for the whole compressor.
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_H
#define _SZ_H

#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>      /* For gettimeofday(), in microseconds */
#include <time.h>          /* For time(), in seconds */
#include "pub.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "VarSet.h"
#include "Huffman.h"
#include "TightDataPointStorageD.h"
#include "TightDataPointStorageF.h"
#include "TightDataPointStorageI.h"
#include "conf.h"
#include "dataCompression.h"
#include "ByteToolkit.h"
#include "TypeManager.h"
#include "sz_int8.h"
#include "sz_int16.h"
#include "sz_int32.h"
#include "sz_int64.h"
#include "sz_uint8.h"
#include "sz_uint16.h"
#include "sz_uint32.h"
#include "sz_uint64.h"
#include "sz_float.h"
#include "sz_double.h"
#include "szd_int8.h"
#include "szd_int16.h"
#include "szd_int32.h"
#include "szd_int64.h"
#include "szd_uint8.h"
#include "szd_uint16.h"
#include "szd_uint32.h"
#include "szd_uint64.h"
#include "szd_float.h"
#include "szd_double.h"
#include "sz_float_pwr.h"
#include "sz_double_pwr.h"
#include "sz_opencl.h"
#include "callZlib.h"
#include "rw.h"
#include "pastri.h"
#include "sz_float_ts.h"
#include "szd_float_ts.h"
#include "utility.h"
#include "CacheTable.h"
#include "MultiLevelCacheTable.h"
#include "MultiLevelCacheTableWideInterval.h"
#include "exafelSZ.h"
#include "sz_stats.h"

#ifdef _WIN32
#define PATH_SEPARATOR ';'
#else
#define PATH_SEPARATOR ':'
#endif

#ifdef __cplusplus
extern "C" {
#endif

//typedef char int8_t;
//typedef unsigned char uint8_t;
//typedef short int16_t;
//typedef unsigned short uint16_t;
//typedef int int32_t;
//typedef unsigned int uint32_t;
//typedef long int64_t;
//typedef unsigned long uint64_t;

#include "defines.h"
	
//Note: the following setting should be consistent with stateNum in Huffman.h
//#define intvCapacity 65536
//#define intvRadius 32768
//#define intvCapacity 131072
//#define intvRadius 65536

#define SZ_COMPUTE_1D_NUMBER_OF_BLOCKS( COUNT, NUM_BLOCKS, BLOCK_SIZE ) \
    if (COUNT <= BLOCK_SIZE){                  \
        NUM_BLOCKS = 1;             \
    }                                   \
    else{                               \
        NUM_BLOCKS = COUNT / BLOCK_SIZE;       \
    }                                   \

#define SZ_COMPUTE_2D_NUMBER_OF_BLOCKS( COUNT, NUM_BLOCKS, BLOCK_SIZE ) \
    if (COUNT <= BLOCK_SIZE){                   \
        NUM_BLOCKS = 1;             \
    }                                   \
    else{                               \
        NUM_BLOCKS = COUNT / BLOCK_SIZE;        \
    }                                   \

#define SZ_COMPUTE_3D_NUMBER_OF_BLOCKS( COUNT, NUM_BLOCKS, BLOCK_SIZE ) \
    if (COUNT <= BLOCK_SIZE){                   \
        NUM_BLOCKS = 1;             \
    }                                   \
    else{                               \
        NUM_BLOCKS = COUNT / BLOCK_SIZE;        \
    }                                   \

#define SZ_COMPUTE_BLOCKCOUNT( COUNT, NUM_BLOCKS, SPLIT_INDEX,       \
                                       EARLY_BLOCK_COUNT, LATE_BLOCK_COUNT ) \
    EARLY_BLOCK_COUNT = LATE_BLOCK_COUNT = COUNT / NUM_BLOCKS;               \
    SPLIT_INDEX = COUNT % NUM_BLOCKS;                                        \
    if (0 != SPLIT_INDEX) {                                                  \
        EARLY_BLOCK_COUNT = EARLY_BLOCK_COUNT + 1;                           \
    }                                                                        \

//typedef unsigned long unsigned long;
//typedef unsigned int uint;

typedef union lint16
{
	unsigned short usvalue;
	short svalue;
	unsigned char byte[2];
} lint16;

typedef union lint32
{
	int ivalue;
	unsigned int uivalue;
	unsigned char byte[4];
} lint32;

typedef union lint64
{
	long lvalue;
	unsigned long ulvalue;
	unsigned char byte[8];
} lint64;

typedef union ldouble
{
    double value;
    unsigned long lvalue;
    unsigned char byte[8];
} ldouble;

typedef union lfloat
{
    float value;
    unsigned int ivalue;
    unsigned char byte[4];
} lfloat;


typedef struct sz_metadata
{
	int versionNumber[3]; //only used for checking the version by calling SZ_GetMetaData()
	int isConstant; //only used for checking if the data are constant values by calling SZ_GetMetaData()
	int isLossless; //only used for checking if the data compression was lossless, used only by calling SZ_GetMetaData()
	int sizeType; //only used for checking whether the size type is "int" or "long" in the compression, used only by calling SZ_GetMetaData()
	size_t dataSeriesLength; //# number of data points in the dataset
	int defactoNBBins; //real number of quantization bins
	struct sz_params* conf_params; //configuration parameters
} sz_metadata;


/*We use a linked list to maintain time-step meta info for time-step based compression*/
typedef struct sz_tsc_metainfo
{
	int totalNumOfSteps;
	int currentStep;
	char metadata_filename[256];
	FILE *metadata_file;
	unsigned char* bit_array; //sihuan added
	size_t intersect_size; //sihuan added
	int64_t* hist_index; //sihuan added: prestep index 

} sz_tsc_metadata;

extern int versionNumber[4];

//-------------------key global variables--------------
extern int dataEndianType; //*endian type of the data read from disk
extern int sysEndianType; //*sysEndianType is actually set automatically.

extern sz_params *confparams_cpr;
extern sz_params *confparams_dec;
extern sz_exedata *exe_params;

//------------------------------------------------
extern SZ_VarSet* sz_varset;
extern sz_multisteps *multisteps; //compression based on multiple time steps (time-dimension based compression)
extern sz_tsc_metadata *sz_tsc;

//for pastri 
#ifdef PASTRI
extern pastri_params pastri_par; 
#endif

//sz.h
HuffmanTree* SZ_Reset();

int SZ_Init(const char *configFilePath);

int SZ_Init_Params(sz_params *params);

size_t computeDataLength(size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

int computeDimension(size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

int SZ_compress_args_float_subblock(unsigned char* compressedBytes, float *oriData,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1,
size_t *outSize, int errBoundMode, double absErr_Bound, double relBoundRatio);

int SZ_compress_args_double_subblock(unsigned char* compressedBytes, double *oriData,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1,
size_t *outSize, int errBoundMode, double absErr_Bound, double relBoundRatio);

unsigned char *SZ_compress(int dataType, void *data, size_t *outSize, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

unsigned char* SZ_compress_args(int dataType, void *data, size_t *outSize, int errBoundMode, double absErrBound, 
double relBoundRatio, double pwrBoundRatio, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

int SZ_compress_args2(int dataType, void *data, unsigned char* compressed_bytes, size_t *outSize, 
int errBoundMode, double absErrBound, double relBoundRatio, double pwrBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

int SZ_compress_args3(int dataType, void *data, unsigned char* compressed_bytes, size_t *outSize, int errBoundMode, double absErrBound, double relBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1);

unsigned char *SZ_compress_rev_args(int dataType, void *data, void *reservedValue, size_t *outSize, int errBoundMode, double absErrBound, double relBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

int SZ_compress_rev_args2(int dataType, void *data, void *reservedValue, unsigned char* compressed_bytes, size_t *outSize, int errBoundMode, double absErrBound, double relBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);
unsigned char *SZ_compress_rev(int dataType, void *data, void *reservedValue, size_t *outSize, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

void SZ_Create_ParamsExe(sz_params** conf_params, sz_exedata** exe_params);

void *SZ_decompress(int dataType, unsigned char *bytes, size_t byteLength, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);
size_t SZ_decompress_args(int dataType, unsigned char *bytes, size_t byteLength, void* decompressed_array, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

sz_metadata* SZ_getMetadata(unsigned char* bytes, sz_exedata* pde_exe);
void SZ_printMetadata(sz_metadata* metadata);


void filloutDimArray(size_t* dim, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

size_t compute_total_batch_size();

void SZ_registerVar(int var_id, char* varName, int dataType, void* data, 
			int errBoundMode, double absErrBound, double relBoundRatio, double pwRelBoundRatio, 
			size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

int SZ_deregisterVar_ID(int var_id);
int SZ_deregisterVar(char* varName);
int SZ_deregisterAllVars();

int SZ_compress_ts_select_var(int cmprType, unsigned char* var_ids, unsigned char var_count, unsigned char** newByteData, size_t *outSize);
int SZ_compress_ts(int cmprType, unsigned char** newByteData, size_t *outSize);
void SZ_decompress_ts_select_var(unsigned char* var_ids, unsigned char var_count, unsigned char *bytes, size_t bytesLength);
void SZ_decompress_ts(unsigned char *bytes, size_t byteLength);

void SZ_Finalize();

void convertSZParamsToBytes(sz_params* params, unsigned char* result);
void convertBytesToSZParams(unsigned char* bytes, sz_params* params, sz_exedata* pde_exe);

unsigned char* SZ_compress_customize(const char* appName, void* userPara, int dataType, void* data, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, int *status);

unsigned char* SZ_compress_customize_threadsafe(const char* cmprName, void* userPara, int dataType, void* data, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, int *status);

void* SZ_decompress_customize(const char* appName, void* userPara, int dataType, unsigned char* bytes, size_t byteLength, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, int* status);

void* SZ_decompress_customize_threadsafe(const char* cmprName, void* userPara, int dataType, unsigned char* bytes, size_t byteLength, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, int *status);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_H  ----- */