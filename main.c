/** **************************************************************************
 * main.c
 * 
 * Copyright 2012 Brian Luczkiewicz <brian@blucz.com>
 * 
 * This file is part of s3rocket.
 * 
 * s3rocket is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, version 3 of the License.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of this library and its programs with the
 * OpenSSL library, and distribute linked combinations including the two.
 *
 * s3rocket is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with s3rocket, in a file named COPYING.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 ****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>

#include <getopt.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <libs3.h>

#ifdef HAVE_GNUTLS
#include <gnutls/gnutls.h>
#include <gcrypt.h>
GCRY_THREAD_OPTION_PTHREAD_IMPL;
#endif

/*
 * Global state from arguments/environment
 */
static int         opt_verbose              = 0;
static int         opt_unencrypted          = 0;
static int64_t     opt_blocksize            = 64 * 1024 * 1024;
static int         opt_jobs                 = 10;
static const char *env_s3_access_key_id     = NULL;
static const char *env_s3_secret_access_key = NULL;
static const char *env_s3_hostname          = NULL;

/*
 * stats
 */
static time_t      start_time               = 0;
static int64_t     bytes_uploaded           = 0;
static int64_t     bytes_downloaded         = 0;

int usage(const char *self) {
    fprintf(stderr, "Usage: %s [OPTION]... put FILE S3BUCKET[:PREFIX]\n", self);
    fprintf(stderr, "\n");
    fprintf(stderr, "       copy a file or stream to S3BUCKET.\n");
    fprintf(stderr, "       Use '-' to read data from stdin\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "  or:  %s [OPTION]... get S3BUCKET[:PREFIX] FILE\n", self);
    fprintf(stderr, "\n");
    fprintf(stderr, "       copy data uploaded using %s from S3BUCKET to FILE.\n", self);
    fprintf(stderr, "       Use '-' to place data on stdout.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    S3BUCKET[:PREFIX] addresses a single unit of data uploaded by %s\n", self);
    fprintf(stderr, "\n");
    fprintf(stderr, "    S3BUCKET is the bucket name on S3. PREFIX is an optional prefix\n");
    fprintf(stderr, "    that should be prepended to object names in that bucket.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Environment:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    S3_ACCESS_KEY_ID     : S3 access key ID (required)\n");
    fprintf(stderr, "    S3_SECRET_ACCESS_KEY : S3 secret access key (required)\n");
    fprintf(stderr, "    S3_HOSTNAME          : specify alternative S3 host (optional)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    -j [N], --jobs=[N]             use up to N threads (default=10)\n");
    fprintf(stderr, "    -b [SIZE], --block-size=[SIZE] set the block size for uploads (default=64M)\n");
    fprintf(stderr, "    -v, --verbose                  print verbose log information\n");
    fprintf(stderr, "    -u, --unencrypted              use HTTP instead of HTTPS\n");
    fprintf(stderr, "    --help                         display help and exit\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Performance Notes:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    The performance of %s depends on large in-memory buffers to smooth out\n", self);
    fprintf(stderr, "    out inconsistent performance of transfers to and from S3 without causing\n");
    fprintf(stderr, "    extra disk I/O. Plan to make available (block-size * jobs) bytes of memory \n");
    fprintf(stderr, "    for the duration of the transfer.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    On fast EC2 machines with 10gbe connections, unencrypted, jobs=20, block-size=64M\n");
    fprintf(stderr, "    seems to give the best throughput.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    In most cases, the bottleneck is going to be the disk that you are reading\n");
    fprintf(stderr, "    from or writing to, or the process consuming/producing the data\n");
    fprintf(stderr, "\n");
    exit(1);
}

static void print_opts(void) {
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    verbose:           1\n");
    fprintf(stderr, "    unencrypted:       %d\n", opt_unencrypted?1:0);
    fprintf(stderr, "    jobs:              %d\n", opt_jobs);
    fprintf(stderr, "    blocksize:         %lld\n", (long long int)opt_blocksize);
    fprintf(stderr, "    access key:        %s\n", env_s3_access_key_id);
    fprintf(stderr, "    secret access key: %s\n", env_s3_secret_access_key);
    fprintf(stderr, "    hostname:          %s\n", env_s3_hostname);
    fprintf(stderr, "\n");
}

void die(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt); 
    fprintf(stderr, "\nERROR: ");
	vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    exit(1);
}

static void check_s3_error(S3Status rc, const char *msg) {
    if (rc != S3StatusOK)
        die("%s: %s", msg, S3_get_status_name(rc));
}

static void init_or_die(void) {
    gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
    gnutls_global_init();

    S3Status rc = S3_initialize("s3rocket", S3_INIT_ALL, env_s3_hostname);
    if (rc != S3StatusOK) {
        die("failed to initialize libs3: %s", S3_get_status_name(rc));
    }
    if (opt_verbose) 
        fprintf(stderr, "Initialized libs3\n");
}

static void die_perror(const char *msg) {
    fprintf(stderr, "\nERROR: ");
    perror(msg);
    exit(1);
}

static void get_bucket_prefix(const char *bucket_prefix, char bucket[/*S3_MAX_BUCKET_NAME_SIZE + 1*/], char prefix[/*S3_MAX_KEY_SIZE + 1*/]) {
    prefix[0] = bucket[0] = 0;
    char *prefix_start = strchr(bucket_prefix, ':');
    if (prefix_start) {
        if (strlen(prefix) >= S3_MAX_KEY_SIZE - 20)
            die("bucket name is too long");
        if (prefix_start - bucket_prefix >= S3_MAX_BUCKET_NAME_SIZE)
            die("prefix name is too long");
        strcpy(prefix, prefix_start + 1);
        strncat(bucket, bucket_prefix, prefix_start - bucket_prefix);
    } else {
        strcat(bucket, bucket_prefix);
    }
}

static S3Status noop_properties_cb(const S3ResponseProperties *properties, void *callbackData) {
    return S3StatusOK;
}

static void status_propagate_complete_cb(S3Status status, const S3ErrorDetails *errorDetails, void *callbackData) {
    S3Status *bucket_status = (S3Status*)callbackData;
    *bucket_status = status;
}

static S3Status test_bucket(const char *bucket) {
    S3ResponseHandler handler = { noop_properties_cb, status_propagate_complete_cb };
    S3Status bucket_status = S3StatusInternalError;
    S3_test_bucket(opt_unencrypted ? S3ProtocolHTTP : S3ProtocolHTTPS,
                   S3UriStyleVirtualHost,
                   env_s3_access_key_id,
                   env_s3_secret_access_key,
                   env_s3_hostname,
                   bucket,
                   0, NULL, /* locationConstraintReturn */
                   NULL,    /* requestContext */
                   &handler,
                   &bucket_status);
    return bucket_status;
}

S3Status create_bucket(const char *bucket) {
    S3ResponseHandler handler = { noop_properties_cb, status_propagate_complete_cb };
    S3Status bucket_status = S3StatusInternalError;
    S3_create_bucket(opt_unencrypted ? S3ProtocolHTTP : S3ProtocolHTTPS,
                     env_s3_access_key_id,
                     env_s3_secret_access_key,
                     env_s3_hostname,
                     bucket,
                     S3CannedAclPrivate,
                     NULL,    /* locationConstraint */
                     NULL,    /* requestContext */
                     &handler,
                     &bucket_status);
    return bucket_status;
}

enum {
    STATE_EMPTY       = 0,
    STATE_FILLING     = 1,
    STATE_FULL        = 2,
    STATE_DONE        = 3,
};

typedef struct {
    const char     *bucket;     /* the bucket to upload to */
    const char     *prefix;     /* the prefix for object names in the bucket */
    int             ordinal;    /* ordinal for upload */

    int             state;      /* PUT_JOB_* */

    void           *ptr;        /* the pointer to the buffer */
    size_t          size;       /* block size */
    size_t          fill;       /* the number of bytes of meaningful data in the buffer */

    pthread_cond_t  cond_full;  /* signalled when the buffer becomes full */
    pthread_cond_t  cond_empty; /* signalled when the buffer becomes empty */

    pthread_mutex_t mutex;

    pthread_t       thread;
} transfer_buf_t;

static transfer_buf_t *allocate_transfer_buffers(int nbufs, int blocksize, const char *bucket, const char *prefix) {
    transfer_buf_t *bufs = (transfer_buf_t*)calloc(nbufs, sizeof(transfer_buf_t)); 
    if (bufs == NULL) return NULL;
    int i;
    for (i = 0; i < nbufs; i++) {
        transfer_buf_t *buf = &bufs[i];
        int rc;

        rc = pthread_mutex_init(&buf->mutex, NULL);
        if (rc != 0) die_perror("failed to create mutex");

        rc = pthread_cond_init(&buf->cond_full, NULL);
        if (rc != 0) die_perror("failed to create condition variable");

        rc = pthread_cond_init(&buf->cond_empty, NULL);
        if (rc != 0) die_perror("failed to create condition variable");

        if (blocksize != 0) {
            buf->ptr = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
            if (!buf->ptr) die_perror("failed to mmap");
        }

        buf->size    = blocksize;
        buf->bucket  = bucket;
        buf->prefix  = prefix;
        buf->ordinal = i;
    }
    return bufs;
}

static void free_transfer_buffers(transfer_buf_t *bufs, int nbufs, int blocksize) {
    int i;
    if (bufs == NULL) return;
    for (i = 0; i < nbufs; i++) {
        transfer_buf_t *buf = &bufs[i];
        pthread_mutex_destroy(&buf->mutex);
        pthread_cond_destroy(&buf->cond_full);
        pthread_cond_destroy(&buf->cond_empty);
        if (buf->ptr)
            munmap(buf->ptr, buf->size);
    }
    free(bufs);
}

typedef struct {
    S3Status      status;
    transfer_buf_t *buf;
    off_t         position;
} upload_callback_data_t;

static void upload_complete_cb(S3Status status, const S3ErrorDetails *errorDetails, void *callbackData) {
    upload_callback_data_t *upload = (upload_callback_data_t*)callbackData;
    upload->status = status;
}

static int put_object_data_cb(int bufferSize, char *buffer, void *callbackData) {
    upload_callback_data_t *upload = (upload_callback_data_t*)callbackData;
    transfer_buf_t *buf = upload->buf;
    int data_remaining = buf->fill - upload->position;
    int tocopy = bufferSize <= data_remaining ? bufferSize : data_remaining;
    memcpy(buffer, (char*)buf->ptr + upload->position, tocopy);
    upload->position += tocopy;

    __sync_fetch_and_add(&bytes_uploaded, tocopy);
    return tocopy;
}

/* Upload a chunk to s3 */
static void upload_chunk(transfer_buf_t *buf) {
    char key[S3_MAX_KEY_SIZE + 1] = {0,};
    snprintf(key, S3_MAX_KEY_SIZE, "%s%s%08d", buf->prefix, buf->prefix[0] ? "-" : "", buf->ordinal);

    S3BucketContext bucketcontext = {0,};
    bucketcontext.hostName        = env_s3_hostname;
    bucketcontext.bucketName      = buf->bucket;
    bucketcontext.protocol        = opt_unencrypted ? S3ProtocolHTTP : S3ProtocolHTTPS;
    bucketcontext.uriStyle        = S3UriStyleVirtualHost;
    bucketcontext.accessKeyId     = env_s3_access_key_id;
    bucketcontext.secretAccessKey = env_s3_secret_access_key;

    S3PutProperties putproperties = {0,};
    putproperties.contentType  = NULL;       /* XXX: make this a cmdline option? */
    putproperties.md5          = NULL;       /* XXX: compute md5 on block before uploading? */
    // XXX: use metadata for consistency checking?

    S3PutObjectHandler handler;
    handler.responseHandler.propertiesCallback = noop_properties_cb;
    handler.responseHandler.completeCallback = upload_complete_cb;
    handler.putObjectDataCallback = put_object_data_cb;

    upload_callback_data_t upload = { S3StatusInternalError, buf, 0 };
    S3_put_object(&bucketcontext, 
                  key, buf->fill,
                  &putproperties,
                  NULL, /* requestContext */
                  &handler, &upload);

    time_t now = time(NULL);
    if (now != start_time) {
        int64_t progress    = bytes_uploaded;
        int64_t progress_mb = progress / (1024 * 1024);
        int64_t mb_per_sec  = progress / (now - start_time) / (1024 * 1024);
        fprintf(stderr, "uploaded chunk #%d (%lldmb in %ds, %lldmb/s)\n", buf->ordinal, (long long int)progress_mb, (int)(now - start_time), (long long int)mb_per_sec);
    }
}

/* Represents one uploader thread */
static void *put_job(void *arg) {
    transfer_buf_t *buf = (transfer_buf_t*)arg;
    int done = 0;

    while (!done) {
        pthread_mutex_lock(&buf->mutex);
        switch (buf->state) {
            case STATE_EMPTY:     
            case STATE_FILLING:   
                pthread_cond_wait(&buf->cond_full, &buf->mutex); 
                break;

            case STATE_FULL:      
                pthread_mutex_unlock(&buf->mutex);
                upload_chunk(buf);
                pthread_mutex_lock(&buf->mutex);
                buf->state = STATE_EMPTY;
                buf->fill = 0;
                pthread_cond_signal(&buf->cond_empty);
                break;

            case STATE_DONE:
                done = 1;
                break;

            default:
                die("internal error");
                break;
        }
        pthread_mutex_unlock(&buf->mutex);
    }

    return NULL;
}

/* Write some data to the appropriate put buffer, making state transitions and/or blocking as necessary. */ 
static void write_put_data(transfer_buf_t *bufs, int nbufs, void *data, size_t count, int *ref_ordinal) {
    while (count > 0) {
        int bufi = *ref_ordinal % nbufs;
        transfer_buf_t *buf = &bufs[bufi];

        pthread_mutex_lock(&buf->mutex);
        switch (buf->state) {
            case STATE_EMPTY:         // the buffer is empty, transition to filling + begin
                buf->state   = STATE_FILLING;
                buf->ordinal = *ref_ordinal;
                /* falls through */

            case STATE_FILLING: {     // put data in the buffer
                size_t space_remaining = buf->size - buf->fill;
                int tocopy = count <= space_remaining ? count : space_remaining;

                char *src    = data;
                char *target = (char*)buf->ptr + buf->fill;

                memcpy(target, src, tocopy);

                buf->fill += tocopy;
                count     -= tocopy;
                data       = src + tocopy;

                if (buf->fill == buf->size) {
                    buf->state = STATE_FULL;
                    pthread_cond_signal(&buf->cond_full);
                    *ref_ordinal = *ref_ordinal + 1;    // advance to next buffer
                }

                break;
            }

            case STATE_FULL:          // if the buffer is full, wait for it to be empty
                pthread_cond_wait(&buf->cond_empty, &buf->mutex);
                break;

            default:
                die("internal error");
                break;
        }
        pthread_mutex_unlock(&buf->mutex);
    }
}

/* This is invoked when tar has finished. Each of the buffers must be marked 'done' for its
 * put_job thread to exit */
static void finish_put_data(transfer_buf_t *bufs, int nbufs, int *ref_ordinal) {
    /* Go through each of the buffers and make sure they have all completed */
    int ordinal;
    for (ordinal = *ref_ordinal; ordinal < *ref_ordinal + nbufs; ordinal++) {
        transfer_buf_t *buf = &bufs[ordinal % nbufs];
        int done = 0;
        while (!done) {
            pthread_mutex_lock(&buf->mutex);

            switch (buf->state) {
                case STATE_EMPTY:         // if the buffer is empty, then mark it done.
                    buf->state = STATE_DONE;
                    pthread_cond_signal(&buf->cond_full);
                    done = 1;
                    break;

                case STATE_FILLING:       // if the buffer is filling, mark it full
                    buf->state = STATE_FULL;
                    pthread_cond_signal(&buf->cond_full);
                    break;

                default:                    // if the buffer is uploading, wait for it to finish
                    pthread_cond_wait(&buf->cond_empty, &buf->mutex);
                    break;
            }

            pthread_mutex_unlock(&buf->mutex);
        }
    }
}

static void cmd_put(const char *file, const char *bucket_prefix) {
    init_or_die();

    start_time = time(NULL);

    char bucket[S3_MAX_BUCKET_NAME_SIZE + 1];
    char prefix[S3_MAX_KEY_SIZE + 1];
    get_bucket_prefix(bucket_prefix, bucket, prefix);

    S3Status rc;

    rc = S3_validate_bucket_name(bucket, S3UriStyleVirtualHost);
    check_s3_error(rc, "invalid bucket name");

    rc = test_bucket(bucket);
    if (rc == S3StatusErrorNoSuchBucket) {
        rc = create_bucket(bucket);
        check_s3_error(rc, "failed to create bucket");
    } else {
        check_s3_error(rc, "invalid bucket");
    }

    transfer_buf_t *bufs = allocate_transfer_buffers(opt_jobs, opt_blocksize, bucket, prefix);
    if (bufs == NULL) die("out of memory");

    int fd;
    if (!strcmp(file, "-")) {
        fd = STDIN_FILENO;
    } else {
        fd = open(file, O_RDONLY);
        if (fd < 0) die_perror("failed to open file");
    }

    int i;
    for (i = 0; i < opt_jobs; i++) {
        transfer_buf_t *buf = &bufs[i];
        pthread_create(&buf->thread, NULL, put_job, buf);
    }

    int ordinal = 0;
    for (;;) {
        char buf[256 * 1024];
        ssize_t bytesread = read(fd, buf, sizeof(buf));
        if (bytesread == 0) break;
        if (bytesread < 0) die_perror("error reading from file");
        write_put_data(bufs, opt_jobs, buf, bytesread, &ordinal);
    }
    finish_put_data(bufs, opt_jobs, &ordinal);

    // join threads
    for (i = 0; i < opt_jobs; i++) {
        transfer_buf_t *buf = &bufs[i];
        pthread_join(buf->thread, NULL);
    }

    close(fd);
    free_transfer_buffers(bufs, opt_jobs, opt_blocksize);
    exit(0);
}

typedef struct {
    S3Status        status;
    transfer_buf_t *buf;
    int64_t         size;
} download_callback_data_t;

static void download_complete_cb(S3Status status, const S3ErrorDetails *errorDetails, void *callbackData) {
    download_callback_data_t *download = (download_callback_data_t*)callbackData;
    download->status = status;
}

static S3Status download_properties_cb(const S3ResponseProperties *properties, void *callbackData) {
    download_callback_data_t *download = (download_callback_data_t*)callbackData;
    transfer_buf_t *buf = download->buf;

    if (properties->contentLength < 0) die("missing content length");

    download->size     = (int64_t)properties->contentLength;

    if (buf->size < download->size) {
        if (buf->ptr)
            munmap(buf->ptr, buf->size);
        buf->ptr = mmap(NULL, download->size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        buf->size = download->size;
        if (!buf->ptr) die_perror("failed to mmap");
    }

    return S3StatusOK;
}

static S3Status get_object_data_cb(int bufferSize, const char *buffer, void *callbackData) {
    download_callback_data_t *download = (download_callback_data_t*)callbackData;
    transfer_buf_t *buf = download->buf;

    if (bufferSize + buf->fill > download->size)
        die("too much data received for block %d (%d > %d)", buf->ordinal, (int)(bufferSize + buf->fill), (int)download->size);

    memcpy((char*)buf->ptr + buf->fill, buffer, bufferSize);
    buf->fill += bufferSize;

    __sync_fetch_and_add(&bytes_downloaded, bufferSize);
    return S3StatusOK;
}

static void *get_job(void *arg) {
    transfer_buf_t *buf = (transfer_buf_t*)arg;
    int done = 0;

    while (!done) {
        // wait for the buffer to be empty 
        pthread_mutex_lock(&buf->mutex);
        while (buf->state != STATE_EMPTY && buf->state != STATE_DONE) {
            pthread_cond_wait(&buf->cond_empty, &buf->mutex);
       }
       if (buf->state == STATE_DONE) {
           done = 1;
           continue;
       }

        buf->state = STATE_FILLING;

        // buffer is empty. now download into it.
        
        char key[S3_MAX_KEY_SIZE + 1] = {0,};
        snprintf(key, S3_MAX_KEY_SIZE, "%s%s%08d", buf->prefix, buf->prefix[0] ? "-" : "", buf->ordinal);

        S3BucketContext bucketcontext = {0,};
        bucketcontext.hostName        = env_s3_hostname;
        bucketcontext.bucketName      = buf->bucket;
        bucketcontext.protocol        = opt_unencrypted ? S3ProtocolHTTP : S3ProtocolHTTPS;
        bucketcontext.uriStyle        = S3UriStyleVirtualHost;
        bucketcontext.accessKeyId     = env_s3_access_key_id;
        bucketcontext.secretAccessKey = env_s3_secret_access_key;

        S3GetObjectHandler handler;
        handler.responseHandler.propertiesCallback = download_properties_cb;
        handler.responseHandler.completeCallback = download_complete_cb;
        handler.getObjectDataCallback = get_object_data_cb;

        download_callback_data_t download = { S3StatusInternalError, buf, 0 };
        S3_get_object(&bucketcontext, key, 
                      NULL,     /* getConditions */
                      0, 0,     /* startByte, byteCount */
                      NULL,     /* requestContext */
                      &handler, &download);

        pthread_mutex_lock(&buf->mutex);
        if (download.status == S3StatusErrorNoSuchKey) {       // we've reached the end
            buf->state = STATE_DONE;
            done = 1;
        } else { 
            check_s3_error(download.status, "download failed");
            buf->state = STATE_FULL;

            if (buf->fill != download.size) die("wrong amount of data received");

            time_t now = time(NULL);
            if (now != start_time) {
                int64_t progress    = bytes_downloaded;
                int64_t progress_mb = progress / (1024 * 1024);
                int64_t mb_per_sec  = progress / (now - start_time) / (1024 * 1024);
                fprintf(stderr, "downloaded chunk #%d (%lldmb in %ds, %lldmb/s)\n", buf->ordinal, (long long int)progress_mb, (int)(now - start_time), (long long int)mb_per_sec);
            }
        }

        pthread_cond_signal(&buf->cond_full);
        pthread_mutex_unlock(&buf->mutex);
    }
    
    return NULL;
}

static void cmd_get(const char *bucket_prefix, const char *file) {
    init_or_die();

    start_time = time(NULL);

    char bucket[S3_MAX_BUCKET_NAME_SIZE + 1];
    char prefix[S3_MAX_KEY_SIZE + 1];
    get_bucket_prefix(bucket_prefix, bucket, prefix);

    S3Status rc;

    rc = S3_validate_bucket_name(bucket, S3UriStyleVirtualHost);
    check_s3_error(rc, "invalid bucket name");

    rc = test_bucket(bucket);
    check_s3_error(rc, "invalid bucket");

    if (rc == S3StatusErrorNoSuchBucket) {
        rc = create_bucket(bucket);
        die("bucket not found");
    } else {
        check_s3_error(rc, "invalid bucket");
    }

    transfer_buf_t *bufs = allocate_transfer_buffers(opt_jobs, 0 /* don't pre-allocate buffers */, bucket, prefix);
    if (bufs == NULL) die("out of memory");

    int fd;
    if (!strcmp(file, "-")) {
        fd = STDOUT_FILENO;
    } else {
        fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) die_perror("failed to open file");
    }

    int i;
    for (i = 0; i < opt_jobs; i++) {
        transfer_buf_t *buf = &bufs[i];
        pthread_create(&buf->thread, NULL, get_job, buf);
    }

    int ordinal;
    for (ordinal = 0; ; ordinal++) {
        transfer_buf_t *buf = &bufs[ordinal % opt_jobs];

        // wait until buffer is full or done
        pthread_mutex_lock(&buf->mutex);

        if (buf->ordinal != ordinal)
            die("internal error");

        int done         = 0;   // the whole copy is done
        int buffer_ready = 0;   // this buffer is ready to be written to the output file
        while (!buffer_ready && !done) {
            switch (buf->state) {
                case STATE_DONE: 
                    if (ordinal == 0) 
                        die("no data found at %s", bucket_prefix);
                    done = 1;
                    break;

                case STATE_FULL:
                    buffer_ready = 1;
                    break;

                case STATE_EMPTY:
                case STATE_FILLING:
                    pthread_cond_wait(&buf->cond_full, &buf->mutex); 
                    break;

                default:
                    die("internal error");
                    break;
            }
        }

        if (done) {
            pthread_mutex_unlock(&buf->mutex);
            break;
        }

        // copy the buffer to the output file
        int off  = 0;
        while (off < buf->fill) {
            int maxcopy        = 256 * 1024 * 1024; 
            int data_remaining = buf->fill - off;
            int tocopy         = data_remaining > maxcopy ? maxcopy : data_remaining;
            int rc = write(fd, (char*)buf->ptr + off, tocopy);
            if (rc <= 0) die_perror("write failed");
            off  += rc;
        }

       //fprintf(stderr, "(%p) advance ordinal %d to %d slot=%d\n", buf, buf->ordinal, buf->ordinal + opt_jobs, ordinal%opt_jobs);
        buf->ordinal += opt_jobs;               // set the ordinal for the next chunk download
        buf->fill  = 0;                         // reset the fill pointer
        buf->state = STATE_EMPTY;               // mark the buffer as empty
        pthread_cond_signal(&buf->cond_empty);  // kick of the next download from this buffer

        pthread_mutex_unlock(&buf->mutex);
    }

    // make sure that all threads die. When non-contiguous data is in s3, sometimes,
    // buffers AFTER the first STATE_DONE buffer are in STATE_FULL
    for (i = 0; i < opt_jobs; i++) {
        transfer_buf_t *buf = &bufs[i];
        pthread_mutex_lock(&buf->mutex);
        if (buf->state == STATE_FULL) {
            buf->state = STATE_DONE;
            pthread_cond_signal(&buf->cond_empty);
        }
        pthread_mutex_unlock(&buf->mutex);
    }

    // join threads
    for (i = 0; i < opt_jobs; i++) {
        transfer_buf_t *buf = &bufs[i];
        pthread_join(buf->thread, NULL);
    }

    close(fd);
    free_transfer_buffers(bufs, opt_jobs, opt_blocksize);
    exit(0);
}

int main(int argc, char **argv) {
    static struct option long_options[] = {
        {"verbose",     no_argument,           0,       'v'},
        {"help",        no_argument,           0,       'h'},
        {"unencrypted", no_argument,           0,       'u'},
        {"jobs",        required_argument,     0,       'j'},
        {"block-size",  required_argument,     0,       'b'},
        {0, 0, 0, 0}
    };

    // for empty args or any arg that maches --help, just print usage
    if (argc == 1) 
        usage(argv[0]);
    int idx = 0;
    for (idx = 0; idx < argc; idx++)
        if (!strcmp(argv[idx], "--help"))
            usage(argv[0]);

    /*
     * Extract needed information from the environment
     */
    env_s3_hostname          = getenv("S3_HOSTNAME");
    if (env_s3_hostname == NULL) env_s3_hostname = S3_DEFAULT_HOSTNAME;

    env_s3_access_key_id     = getenv("S3_ACCESS_KEY_ID");
    if (env_s3_access_key_id == NULL || !*env_s3_access_key_id)
        die("missing environment variable S3_ACCESS_KEY_ID");

    env_s3_secret_access_key = getenv("S3_SECRET_ACCESS_KEY");
    if (env_s3_secret_access_key == NULL || !*env_s3_secret_access_key)
        die("missing environment variable S3_SECRET_ACCESS_KEY");

    /* 
     * Parse command line options 
     */ 
    idx = 0;
    for (;;) {
        int c = getopt_long(argc, argv, "vhcuj:b:", long_options, &idx);
        if (c == -1) break;

        switch (c) {
            case 'h': usage(argv[0]);                     break;
            case 'v': opt_verbose = 1;                    break;
            case 'u': opt_unencrypted = 1;                break;
            case 'j': 
                opt_jobs = atoi(optarg);
                if (opt_jobs == 0)
                    die("invalid jobs argument: '%s'", optarg);
                break;

            case 'b': { 
                int     arglen = strlen(optarg);
                int64_t mult   = 1;
                char   *endptr;
                switch (optarg[arglen-1]) {
                    case 'k': case 'K': arglen--; mult = 1024;               break;
                    case 'm': case 'M': arglen--; mult = 1024 * 1024;        break;
                    case 'g': case 'G': arglen--; mult = 1024 * 1024 * 1024; break;
                    default: break;
                }
                endptr = optarg+arglen;
                opt_blocksize = mult * strtoll(optarg, &endptr, 10);
                if (endptr == optarg) {
                    die("invalid block-size argument: '%s'", optarg);
                }
                break;
            }

            case '?':
                exit(1);
                break;

            default:
                die("Unknown option: -%c", c);
                break;
        }
    }

    if (optind == argc)
        die("expected get or put command");

    /* 
     * Parse the user's command
     */
    const char *cmd_arg = argv[optind++];
    if (!strcmp("get", cmd_arg)) {
        if (optind + 2 != argc) die("get expects two arguments");
        const char *bucket_prefix = argv[optind++];
        const char *file    = argv[optind++];
        if (opt_verbose) print_opts();
        cmd_get(bucket_prefix, file);

    } else if (!strcmp("put", cmd_arg)) {
        if (optind + 2 != argc) die("put expects two arguments");
        const char *file           = argv[optind++];
        const char *bucket_prefix = argv[optind++];
        if (opt_verbose) print_opts();
        cmd_put(file, bucket_prefix);

    } else {
        die("unknown command: '%s'", cmd_arg);
    }

    return 0;
}
