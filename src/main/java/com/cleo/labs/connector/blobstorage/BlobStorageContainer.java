package com.cleo.labs.connector.blobstorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.EnumSet;

import org.apache.commons.io.FilenameUtils;

import com.cleo.connector.api.property.ConnectorPropertyException;
import com.google.common.base.Strings;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerProperties;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class BlobStorageContainer {
    private CloudStorageAccount account;
    private CloudBlobClient client;
    private CloudBlobContainer container;
    private OperationContext context;

    /**
     * Delimiter for Blob directory hierarchy
     */
    private String delimiter;

    /**
     * Pattern matching NON-EMPTY strings that do NOT end with {@code /}.
     */
    private String not_ending_with_delimiter;

    /**
     * Returns a folder, normalized to end with / if it doesn't already (and if
     * it's not empty).
     * 
     * @param folder
     * @return folder/
     */
    public String normalizePath(String folder) {
        if (!Strings.isNullOrEmpty(folder)) {
            folder = folder.replaceFirst(not_ending_with_delimiter, delimiter);
        }
        return folder;
    }

    public BlobStorageContainer(BlobStorageConnectorConfig config)
            throws ConnectorPropertyException, InvalidKeyException, URISyntaxException, StorageException {
        context = config.getOperationContext();
        account = CloudStorageAccount.parse(config.getConnectionString());
        client = account.createCloudBlobClient();
        delimiter = client.getDirectoryDelimiter();
        not_ending_with_delimiter = "(?<=[^" + delimiter + "])$";
        container = client.getContainerReference(config.getContainer());
    }

    /**
     * Returns the container properties
     * 
     * @return
     */
    public BlobContainerProperties getProperties() {
        return container.getProperties();
    }

    /**
     * Returns a reference to an (existing) blob.
     * 
     * @param path
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     */
    public CloudBlob getBlob(String path) throws URISyntaxException, StorageException {
        return container.getBlobReferenceFromServer(path, null /* snapshotID */, null /* accessCondition */,
                null /* options */, context);
    }

    /**
     * Opens a blob for reading.
     * 
     * @param path
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     */
    public InputStream getInputStream(String path) throws URISyntaxException, StorageException {
        return getBlob(path).openInputStream();
    }

    /**
     * Open a blob for writing, possibly making a unique name.
     * 
     * @param path
     * @param unique
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     */
    public OutputStream getOutputStream(String path, boolean append, boolean unique) throws URISyntaxException, StorageException, IOException {
        // first assess the exitence and type of the blob
        // throws IOException if the type is not BLOCK for overwrite (!append) or APPEND for append
        CloudBlob test = null;
        try {
            test = container.getBlobReferenceFromServer(path, null, null, null, context);
            BlobType type = test.getProperties().getBlobType();
            if (append && type != BlobType.APPEND_BLOB) {
                throw new IOException("unsupported Blob type for operation: "+BlobType.APPEND_BLOB+" required.");
            } else if (!append && type != BlobType.BLOCK_BLOB) {
                throw new IOException("unsupported Blob type for operation: "+BlobType.BLOCK_BLOB+" required.");
            }
System.err.println(path+" is of type "+type.name());
        } catch (StorageException e) {
            if (!e.getErrorCode().equals(StorageErrorCodeStrings.BLOB_NOT_FOUND)) {
System.err.println(path+" has this problem: "+e.getErrorCode()+", not: "+StorageErrorCodeStrings.BLOB_NOT_FOUND);
                throw e;
            }
System.err.println(path+" does not exist");
        }
        // test means not existing
        // make it unique if requested
        if (unique && test != null) {
            int counter = 0;
            String ext = FilenameUtils.getExtension(path).replaceFirst("^(?=[^\\.])","."); // prefix with "." unless empty or already "."
            String base = path.substring(0, path.length()-ext.length());
            String candidate;

            do {
                counter++;
                candidate = base+"."+counter+ext;
                try {
                    test = container.getBlobReferenceFromServer(candidate, null /* snapshotID */, null /* accessCondition */, null /* options */, context);
                } catch (StorageException e) {
                    if (!e.getErrorCode().equals(StorageErrorCodeStrings.BLOB_NOT_FOUND)) {
                        throw e;
                    }
                    test = null;
                }
            } while (test != null);
            path = candidate;
        }
        // test and path are now updated for uniqueness
        // test will be non-null if !unique
        if (append) {
            BlobRequestOptions options = new BlobRequestOptions();
            options.setAbsorbConditionalErrorsOnRetry(true); // advised for single writer scenarios
            if (test == null) {
                return container.getAppendBlobReference(path).openWriteNew(null /* accessCondition */, options, context);
            } else {
                return container.getAppendBlobReference(path).openWriteExisting(null /* accessCondition */, options, context);
            }
        } else {
            return container.getBlockBlobReference(path).openOutputStream(null /* accessCondition */, null /* options */, context);
        }
    }

    /**
     * Makes a "folder" by creating an empty Blob with "folder/" as the name.
     * 
     * @param container
     * @param folder
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IOException
     */
    public void mkdir(String folder) throws URISyntaxException, StorageException, IOException {
        if (!Strings.isNullOrEmpty(folder)) {
            folder = normalizePath(folder);
            CloudBlockBlob blob = container.getBlockBlobReference(folder);
            BlobOutputStream bos = blob.openOutputStream(null /* accessCondition */, null /* options */, context);
            bos.close();
        }
    }

    /**
     * Lists the blobs with the specified prefix.
     * 
     * @param folder
     * @return
     */
    public Iterable<ListBlobItem> dir(String folder) {
        folder = normalizePath(folder);
        return container.listBlobs(folder, false /* useFlatBlobListing */, EnumSet.noneOf(BlobListingDetails.class),
                null /* options */, context);
    }

    /**
     * Returns true if the folder is empty
     * 
     * @param container
     * @param folder
     * @return true if empty, false if at least one entry
     */
    public boolean isempty(String folder) {
        for (ListBlobItem item : dir(folder)) {
            if (!(item instanceof CloudBlob) || !((CloudBlob) item).getName().equals(folder)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Removes a directory, as long as it is not empty
     * 
     * @param container
     * @param folder
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IOException
     */
    public void rmdir(String folder) throws URISyntaxException, StorageException, IOException {
        if (!Strings.isNullOrEmpty(folder)) {
            folder = normalizePath(folder);
            if (!isempty(folder)) {
                throw new IOException(String.format("the directory \"%s\" is not empty", folder));
            }
            CloudBlob blob = container.getBlobReferenceFromServer(folder, null /* snapshotID */, null /* accessCondition */,
                    null /* options */, context);
            blob.delete(DeleteSnapshotsOption.NONE, null /* accessCondition */, null /* options */, context);
        }
    }
}