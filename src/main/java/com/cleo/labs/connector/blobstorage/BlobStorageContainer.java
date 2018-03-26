package com.cleo.labs.connector.blobstorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Strings;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerProperties;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class BlobStorageContainer {
    private BlobStorageAccount account;
    private CloudBlobContainer container;

    public BlobStorageContainer(BlobStorageAccount account, String name)
            throws URISyntaxException, StorageException {
        this.account = account;
        this.container = account.client().getContainerReference(name);
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
                null /* options */, account.context());
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
        // first assess the existence and type of the blob
        // throws IOException if the type is not BLOCK for overwrite (!append) or APPEND for append
        CloudBlob test = null;
        try {
            test = container.getBlobReferenceFromServer(path, null, null, null, account.context());
            BlobType type = test.getProperties().getBlobType();
            if (append && type != BlobType.APPEND_BLOB) {
                throw new IOException("unsupported Blob type for operation: "+BlobType.APPEND_BLOB+" required.");
            } else if (!append && type != BlobType.BLOCK_BLOB) {
                throw new IOException("unsupported Blob type for operation: "+BlobType.BLOCK_BLOB+" required.");
            }
        } catch (StorageException e) {
            if (!e.getErrorCode().equals(StorageErrorCodeStrings.BLOB_NOT_FOUND)) {
                throw e;
            }
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
                    test = container.getBlobReferenceFromServer(candidate, null /* snapshotID */, null /* accessCondition */, null /* options */, account.context());
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
                return container.getAppendBlobReference(path).openWriteNew(null /* accessCondition */, options, account.context());
            } else {
                return container.getAppendBlobReference(path).openWriteExisting(null /* accessCondition */, options, account.context());
            }
        } else {
            return container.getBlockBlobReference(path).openOutputStream(null /* accessCondition */, null /* options */, account.context());
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
            folder = account.normalizePath(folder);
            CloudBlockBlob blob = container.getBlockBlobReference(folder);
            BlobOutputStream bos = blob.openOutputStream(null /* accessCondition */, null /* options */, account.context());
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
        folder = account.normalizePath(folder);
        return container.listBlobs(folder, false /* useFlatBlobListing */, EnumSet.noneOf(BlobListingDetails.class),
                null /* options */, account.context());
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
            folder = account.normalizePath(folder);
            if (!isempty(folder)) {
                throw new IOException(String.format("the directory \"%s\" is not empty", folder));
            }
            CloudBlob blob = container.getBlobReferenceFromServer(folder, null /* snapshotID */, null /* accessCondition */,
                    null /* options */, account.context());
            blob.delete(DeleteSnapshotsOption.NONE, null /* accessCondition */, null /* options */, account.context());
        }
    }
}