package main.common;

public class Constants {
    // representing an invalid page id
    public static final int INVALID_PAGE_ID  = -1;
    public static final int PAGE_SIZE  = 512;     // size of a data page in byte
    public static final int HEADER_PAGE_ID = 0;

//#define INVALID_TXN_ID -1  // representing an invalid txn id
//#define INVALID_LSN -1     // representing an invalid lsn
//#define HEADER_PAGE_ID 0   // the header page id
//#define LOG_BUFFER_SIZE                                                            \
//        ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE) // size of a log buffer in byte
//#define BUCKET_SIZE 50                 // size of extendible hash bucket
//#define BUFFER_POOL_SIZE 10            // size of buffer pool
//
//        typedef int32_t page_id_t; // page id type
//        typedef int32_t txn_id_t;  // transaction id type
//        typedef int32_t lsn_t;     // log sequence number type
//
//    }
}
