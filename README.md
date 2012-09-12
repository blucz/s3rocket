## s3rocket ##

s3rocket uploads and downloads large volumes of streaming data to/from amazon
s3 very quickly.

You don't need to know the size of the data in advance in order to upload it.
The data is split among s3 objects of a fixed block size, and is uploaded on
the fly in multiple threads.

As such, it is excellent for uploading large directory trees or data that
streams out of a process. It is especially excellent when run from inside the
ec2 universe, where ample bandwidth to/from s3 is a given.

This tool was initially designed to upload or download ~50gb of data spread
across ~100,000 files to or from an EC2 instance in 10 minutes or less.

It is overkill for small files. If you're not transferring at least several
gigabytes of data in each go, this is probably not for you.


## Administrivia ##

s3rocket is released under the GNU GPLv3. See the file COPYING for more
information.

s3rocket was created by Brian Luczkiewicz <brian@blucz.com>

Git repository: http://github.com/blucz/s3rocket


## Installing ##

### Supported Platforms: ###

- Linux
- Mac OS X

### Dependencies: ###

* libs3   >= 2.0

  I recommend building libs3 from source. You can get it at http://libs3.ischo.com.s3.amazonaws.com/index.html

* libcurl

  Make sure you have header files installed for libcurl. On some systems,
  you will need to install a separate "-dev" or "-devel" package. If you
  have a 'curl-config' command, you're probably fine.

* gnutls, gcrypt (linux only)

  libcurl has an unfortunate tendency to depend on gnutls and gcrypt on
  some linux system. Even worse, gnutls blows up in multi-threaded apps
  unless you initialize it very carefully.

  The s3rocket makefile assumes that this initialization is needed on linux,
  since popular distributions seem to require it.
   
  As a result, you'll need dev packages for gnutls and gcrypt as well.

  If you want to disable the gnutls/gcrypt initialization, perhaps because
  you know that your libcurl uses an alternative SSL library or because you
  don't intend to use SSL, you can do so by editing the makefile.

Building s3rocket:

    $ make && sudo make install

## Usage ##

    Usage: s3rocket [OPTION]... put FILE S3BUCKET[:PREFIX]

           copy a file or stream to S3BUCKET.
           Use '-' to read data from stdin stdin

      or:  s3rocket [OPTION]... get S3BUCKET[:PREFIX] FILE

           copy data uploaded using s3rocket from S3BUCKET to FILE.
           Use '-' to place data on stdout.

        S3BUCKET[:PREFIX] addresses a single unit of data uploaded by s3rocket

        S3BUCKET is the bucket name on S3. PREFIX is an optional prefix
        that should be prepended to object names in that bucket.

    Environment:

        S3_ACCESS_KEY_ID     : S3 access key ID (required)
        S3_SECRET_ACCESS_KEY : S3 secret access key (required)
        S3_HOSTNAME          : specify alternative S3 host (optional)

    Options:

        -j [N], --jobs=[N]             use up to N threads (default=10)
        -b [SIZE], --block-size=[SIZE] set the block size for uploads (default=64M)
        -v, --verbose                  print verbose log information
        -u, --unencrypted              use HTTP instead of HTTPS
        --help                         display help and exit

    Performance Notes:

        The performance of s3rocket depends on large in-memory buffers to smooth out
        out inconsistent performance of transfers to and from S3 without causing
        extra disk I/O. Plan to make available (block-size * jobs) bytes of memory 
        for the duration of the transfer.

        On fast EC2 machines with 10gbe connections, unencrypted, jobs=20, block-size=64M
        seems to give the best throughput.

        In most cases, the bottleneck is going to be the disk that you are reading
        from or writing to, or the process consuming/producing the data


## Examples ##

#### Upload a directory tree to s3 ####

This example disables https, splits the file into 64mb blocks, and uploads
in 20 threads. Objects are placed in `s3://test/foo-00000000`,
`s3://test/foo-00000001`, ...

    tar cf - my_directory | s3rocket -u -j 20 -b 64M put - test:foo


#### Download a directory tree from s3 ####

This example disables https and downloads in 20 threads.

    s3rocket -u -j 20 get test:foo - | tar xf -
