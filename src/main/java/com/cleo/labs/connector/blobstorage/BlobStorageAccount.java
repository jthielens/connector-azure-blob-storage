package com.cleo.labs.connector.blobstorage;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.regex.Pattern;

import com.cleo.connector.api.property.ConnectorPropertyException;
import com.google.common.base.Strings;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ContainerListingDetails;

public class BlobStorageAccount {
    private CloudStorageAccount account;
    private CloudBlobClient client;
    private OperationContext context;

    public BlobStorageAccount(BlobStorageConnectorConfig config)
            throws ConnectorPropertyException, InvalidKeyException, URISyntaxException {
        context = config.getOperationContext();
        account = CloudStorageAccount.parse(config.getConnectionString());
        client = account.createCloudBlobClient();
        delimiter = client.getDirectoryDelimiter();
        not_ending_with_delimiter = "(?<=[^" + delimiter + "])$";
    }

    public OperationContext context() {
        return context;
    }

    public CloudBlobClient client() {
        return client;
    }

    public BlobStorageContainer getContainer(String name)
            throws URISyntaxException, StorageException {
        return new BlobStorageContainer(this, name);
    }

    public Iterable<CloudBlobContainer> dir() {
        return client.listContainers(null, ContainerListingDetails.METADATA, null /* options */, context);
    }

    /**
     * Delimiter for Blob directory hierarchy
     */
    String delimiter;

    /**
     * Pattern matching NON-EMPTY strings that do NOT end with {@code /}.
     */
    String not_ending_with_delimiter;

    /**
     * Returns a folder, normalized to end with / if it doesn't already (and if
     * it's not empty).
     * 
     * @param blobStorageContainer TODO
     * @param folder
     * @return folder/
     */
    public String normalizePath(String folder) {
        if (!Strings.isNullOrEmpty(folder)) {
            folder = folder.replaceFirst(not_ending_with_delimiter, delimiter);
        }
        return folder;
    }

    /**
     * Represents a container and path, given a (possibly empty/null) container
     * and (possibly empty/null) path.
     */
    public class ContainerAndPath {
        public BlobStorageContainer container;
        public String path;
        public ContainerAndPath(BlobStorageContainer container, String path)
                throws URISyntaxException, StorageException {
            if (container != null) {
                this.container = container;
                this.path = path;
            } else if (Strings.isNullOrEmpty(path)) {
                this.container = null;
                this.path = null;
            } else {
                String[] parts = path.split(Pattern.quote(delimiter), 2);
                this.container = getContainer(parts[0]);
                this.path = parts.length > 1 ? parts[1] : "";
            }
        }
    }

    public ContainerAndPath parse(BlobStorageContainer container, String path)
            throws URISyntaxException, StorageException {
        return new ContainerAndPath(container, path);
    }
}