## Environment Variables

The use of these Environment Variables will alter the behavior of TileDB. These are meant to be experimental and may be removed in future when the functionality is main streamed.

* TILEDB_DISABLE_FILE_LOCKING
     Relevant only for PosixFS. No read/write locks are maintained for arrays. If used, it is the responsibility of the client to ensure that creating/updating/deleting arrays and array fragments are done with utmost care.
* TILEDB_KEEP_FILE_HANDLES_OPEN
     Relevant only for PosixFS. All file handles during writes are kept open until a PosixFS::close_file() is called.


* TILEDB_UPLOAD_BUFFER_SIZE
     Helps write out buffered array fragments to the datastore. If this is set to 0(default for PosixFS and HDFS), array fragments are written out immediately.
* TILEDB_DOWNLOAD_BUFFER_SIZE
     Helps prefetch/read from buffered array fragments from the datastore. If this is set to 0(default for PosixFS and HDFS), array fragments are read unbuffered.

* TILEDB_MAX_STREAM_SIZE
     For azure blob storage, use download_blob_to_stream to read lengths < TILEDB_MAX_STREAM_SIZE. If this is not set, the default is 1024 bytes defined in core/include/storage_manager/storage_azure_blob.h.
