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


Run s3rocket with --help for detailed usage informatio

#### Upload a directory tree to s3 ####

This example disables https, splits the file into 64mb blocks, and uploads
in 20 threads. Objects are placed in `s3://test/foo-00000000`,
`s3://test/foo-00000001`, ...

    tar cf - my_directory | s3rocket -u -j 20 -b 64M put - test:foo


#### Download a directory tree from s3 ####

This example disables https and downloads in 20 threads.

    s3rocket -u -j 20 get test:foo - | tar xf -
