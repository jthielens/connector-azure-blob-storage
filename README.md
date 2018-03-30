# README #

This connector supports Microsoft Azure Blob Storage as a plugin connector
for Harmony 5.5 or Harmony 5.4.1.

## TL;DR ##

<!---
The POM for this project creates a ZIP archive intended to be expanded from
the Harmony installation directory (`$CLEOHOME` below).

```
git clone git@github.com:jthielens/connector-azure-blob-storage.git
mvn clean package
cp target/blob-5.4.1.0-SNAPSHOT-distribution.zip $CLEOHOME
cd $CLEOHOME
unzip -o blob-5.4.1.0-SNAPSHOT-distribution.zip
./Harmonyd stop
./Harmonyd start
```
--->

The Azure Blob Storage connector is distributed as a ZIP archive named
`blob-5.4.1.0-build-distribution.zip` where `build` is a build number
(of 7 heaxadecial digits).  It can be installed on a Harmony 5.5 server,
or on a Harmony 5.4.1 server at patch level 9 (5.4.1.9) or higher.

To install the connector, expand the archive from the Harmony 5.5 installation
directory (`$CLEOHOME` below) and restart Harmony.

```
cd $CLEOHOME
unzip -o blob-5.4.1.0-build-distribution.zip
./Harmonyd stop
./Harmonyd start
```

or for Harmony 5.4.1

```
cd $CLEOHOME
unzip -o blob-5.4.1.0-build-distribution-54.zip
./Harmonyd stop
./Harmonyd start
```

When Harmony/VLTrader restarts, you will see a new `Template` in the host tree
under `Connections` > `Generic` > `Generic BLOB`.  Select `Clone and Activate`
and a new `BLOB` connection (host) will appear on the `Active` tab.

To configure the new `BLOB` connection, enter your Azure Storage Account, one of your Access Keys, and (optionally) the name of your Container in the `BLOB` panel.  You may also configure the Proxy Address, Proxy Port, and additional HTTP Headers if these are required to access the Azure cloud from your Harmony instance.

Each `BLOB` connection corresponds either to an entire storage account (if the container name
is not supplied in the connection), or to a single named container in a single storage account.  Although the default alias for the connection is `BLOB`, it is most
intuitive (but not required) to use the storage account name or the container name for
the alias.
You may repeat the `Clone and Activate` process, or you may `Clone...` an existing `BLOB` connection, to create connections to additional containers and storage accounts.


## Connector Actions ##

Actions configured directly for a Blob connection may directly manipulate the
associated container through _commands_.  The following commands (and options)
are supported:

| Command | Options | Description |
|---------|---------|-------------|
| `DIR` _directory_    | &nbsp; | List the contents of a (virtual) directory.  Use `DIR ""` to list contents of the account or container root |
| `GET`&nbsp;_name_&nbsp;_destination_ | `-DEL` | Retrieve the contents of Blob _name_ into _destination_, subsequently deleting the Blob if `-DEL` is set. |
| `PUT` _source_ _name_ | `-APE`<br/>`-DEL`<br/>`-UNI` | Store the contents of _source_ into Blob _name_, subsequently deleting _source_ if `-DEL` is set.  See *Blob Types* below for a discussion of the `-APPend` and `-UNIque` options. |
| `DELETE` _name_ | &nbsp; | Deletes Blob _name_ from the container. |
| `ATTR` _name_ | &nbsp; | Retrieves the attributes of Blob _name_. |
| `MKDIR` _name_ | &nbsp; | Creates a placeholder Block Blob _name_`/` (appending the directory separator if needed). |
| `RMDIR` _name_ | &nbsp; | Deletes a placeholder Block Blob _name_`/` (appending the directory separator if needed) if it exists and no additional Blobs exist with _name_`/` as a prefix. |

For a storage account level connection (one for which no specific container name is
specified), the top-level directory name is mapped to the container name.  This means
that `MKDIR container` and `RMDIR container` can be used to create and delete containers
within the storage account.

For a container level connection (where a container name is specified), the container
itself may not be manipulated.  Specifically `RMDIR ""` is an error, not an attempt
to delete the container.

### Unique Filenames ###

If the `-UNIque` option is used with the connector `PUT` command, the connector
will insert a uniqueness token into the filename
until it can find an unclaimed filename.  Uniqueness tokens include a leading `.`
followed by a sequential counter, e.g. `.1`, `.2`, etc.

The uniqueness token
is inserted just before the filename extension, or at the end of the
filename if there is no extension, e.g.

* `filename.txt` &rarr; `filename.1.txt` &rarr; `filename.2.txt` &hellip;
* `filename` &rarr; `filename.1` &rarr; `filename.2` &hellip;

## Azure Blob Support ##

The connector projects an image of a traditional hierarchical file system
through its `DIR`, `GET`, `PUT`, and other commands.  Azure Blob Storage is
not, however, exactly a traditional hierarchical file system.

### Folders ###

Azure Blob Storage supports a flat namespace and directories are considered
"virtual" using a _directory delimiter_ that by default is `/`.  The connector
uses the default directory delimiter.

By convention, a directory in Azure Blob Storage does not truly _exist_, but
is inferred when blobs exist with a name ending in the delimiter as a prefix,
e.g. a blob named `a/b/blob` will cause directories `a/` and `a/b/` to be
inferred.  The connector will display inferred directories in response to a
`DIR` command.

In order to simulate persistent directories (which could be empty), the
connector uses the common Azure convention of creating zero-length block
blobs with names ending in the delimiter.  These pseudo-directory block blobs
are created and deleted with the connector's `MKDIR` and `RMDIR` commands.

### Blob Types ###

Azure Storage offers [three types of blob storage](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview): Block Blobs, Append Blobs and Page Blobs.

* Block blobs are composed of blocks and are ideal for storing text or binary files, and for uploading large files efficiently.
* Append blobs are also made up of blocks, but they are optimized for append operations, making them ideal for logging scenarios.
* Page blobs are made up of 512-byte pages up to 8 TB in total size and are more efficient for frequent random read/write operations.

The connector supports all three types of Blob for read (`GET`) operations.
When writing to new or existing blobs, the blob type must be indicated.  In
the connector, Page Blobs are not supported for writing.  Block Blobs and Append
Blobs are distinguished through the `-APPend` flag on the `PUT` command.  The `UNIque` flag also has an impact on blob naming and creation.

* `PUT name` creates a new Block Blob or overwites one if `name` already exists.
* `PUT -UNI name` creates a new Block Blob, creating a unique name based on `name` if `name` already exists.
* `PUT -APP name` creates a new Append Blob, or appends to an existing one if `name` already exists (causing an error if `name` is not an Append Blob).
* `PUT -APP -UNI name` creates a new Append Blob, creating a unique name based on `name` if `name` already exists.


### Rename ###

_Rename_ in Azure Blob Storage is a simulated operation, implemented (for example
by the [Microsoft Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)) as a copy folowed by a delete.  The
connector does not currently support `RENAME`.

## Use as URI ##

You may refer to a Blob connection from actions outside of the Blob connection
itself using _URI_ references of the form `blob:alias/container/path` or `blob:alias/path`
(where `alias` is the alias assigned to the connection, typically the account or container
name).  These URI references can appear in most places where a filename can appear, including:

* as the destination of a `GET` command for another connection (which will stream the retrieved content from that connection into a `PUT` command for the Blob connection).
* as the source of a `PUT` command for another conneciton (which will stream into that connection the content retrieved from a `GET` command for the Blob connection).
* as the source of an `LCOPY` opertation (which will stream the content from a Blob `GET` to the desginated destination).
* as the destination of an `LCOPY` operation (which will stream the content from the designated source to the Blob `PUT` command).
* as the Home Directory of a User, which will map the user's inbox, outbox, and other directories into a Blob container.
* as a _virtual folder_ in the User Upload Folder, User Download Folder, or Other Folders for a User, using the syntax `virtualFolderName=blob:alias/path` or `virtualFolderName=blob:alias/path(permissions)`, which will map all or part of a container into a User's folders.

When used as a virtual folder, `permissions` is a comma-separated list of permissions&mdash;`LIST`, `MKDIR`, `RMDIR`, `READ`, `WRITE`, `OVERWERITE`, `DELETE`&mdash;, or `ALL` (which is the default).  Note that the `MVDIR` and `RENAME` permissions, if specified, will not apply since the connector does not emulate renaming in Azure Blob Storage.

