package com.cleo.labs.connector.blobstorage;

import static com.cleo.connector.api.command.ConnectorCommandName.ATTR;
import static com.cleo.connector.api.command.ConnectorCommandName.DELETE;
import static com.cleo.connector.api.command.ConnectorCommandName.DIR;
import static com.cleo.connector.api.command.ConnectorCommandName.GET;
import static com.cleo.connector.api.command.ConnectorCommandName.MKDIR;
import static com.cleo.connector.api.command.ConnectorCommandName.PUT;
import static com.cleo.connector.api.command.ConnectorCommandName.RMDIR;
import static com.cleo.connector.api.command.ConnectorCommandOption.Append;
import static com.cleo.connector.api.command.ConnectorCommandOption.Delete;
import static com.cleo.connector.api.command.ConnectorCommandOption.Unique;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.cleo.connector.api.ConnectorClient;
import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.annotations.Command;
import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.command.ConnectorCommandUtil;
import com.cleo.connector.api.command.DirCommand;
import com.cleo.connector.api.command.GetCommand;
import com.cleo.connector.api.command.OtherCommand;
import com.cleo.connector.api.command.PutCommand;
import com.cleo.connector.api.directory.Directory.Type;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.helper.Attributes;
import com.cleo.connector.api.interfaces.IConnectorIncoming;
import com.cleo.connector.api.interfaces.IConnectorOutgoing;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.labs.connector.blobstorage.BlobStorageAccount.ContainerAndPath;
import com.google.common.base.Strings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class BlobStorageConnectorClient extends ConnectorClient {
    private BlobStorageConnectorConfig config;
    private BlobStorageAccount account;
    private BlobStorageContainer container;

    /**
     * Constructs a new {@code BlobStorageConnectorClient} for the schema using
     * the default config wrapper and LexFileFactory.
     * 
     * @param schema the {@code BlobStorageConnectorSchema}
     */
    public BlobStorageConnectorClient(BlobStorageConnectorSchema schema) {
        this.config = new BlobStorageConnectorConfig(this, schema);
        this.container = null;
    }

    /**
     * Establishes a live container reference from the configuration.
     * 
     * @throws InvalidKeyException
     * @throws ConnectorPropertyException
     * @throws URISyntaxException
     * @throws StorageException
     */
    private synchronized void setup() throws InvalidKeyException, ConnectorPropertyException, URISyntaxException, StorageException {
        if (container == null) {
            logger.debug("connecting as "+config.getConnectionString());
            logger.debug("proxy is "+config.getProxy());
            account = new BlobStorageAccount(config);
            if (!Strings.isNullOrEmpty(config.getContainer())) {
                container = account.getContainer(config.getContainer());
            }
        }
    }

    @Command(name = DIR)
    public ConnectorCommandResult dir(DirCommand dir)
            throws ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        String path = dir.getSource().getPath();

        logger.debug(String.format("DIR '%s'", path));
        setup();

        if (path.equals(".")) path = ""; // TODO: remove when Harmony is fixed
        ContainerAndPath cp = account.parse(container, path);
        cp.path = account.normalizePath(cp.path);

        List<Entry> list = new ArrayList<>();
        if (cp.container == null) {
            for (CloudBlobContainer c : account.dir()) {
                Entry entry = new Entry(Type.dir)
                        .setPath(c.getName())
                        .setDate(Attributes.toLocalDateTime(c.getProperties().getLastModified()))
                        .setSize(-1L);
                list.add(entry);
            }
        } else {
            for (ListBlobItem item : cp.container.dir(cp.path)) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory directory = (CloudBlobDirectory) item;
                    Entry entry = new Entry(Type.dir).setPath(directory.getPrefix().substring(cp.path.length())).setSize(-1L);
                    list.add(entry);
                } else if (item instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) item;
                    if (!blob.getName().equals(cp.path)) { // the directory placeholder
                                                        // path/ is omitted
                        BlobProperties properties = blob.getProperties();
                        Entry entry = new Entry(Type.file).setPath(blob.getName().substring(cp.path.length()))
                                .setSize(properties.getLength())
                                .setDate(Attributes.toLocalDateTime(properties.getLastModified()));
                        list.add(entry);
                    }
                }
            }
        }
        return new ConnectorCommandResult(Status.Success, Optional.empty(), list);
    }

    @Command(name = GET, options = { Delete })
    public ConnectorCommandResult get(GetCommand get) throws
            ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        String path = get.getSource().getPath();
        IConnectorIncoming destination = get.getDestination();

        logger.debug(String.format("GET remote '%s' to local '%s'", path, destination.getPath()));
        setup();
        ContainerAndPath cp = account.parse(container, path);

        if (cp.container == null) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        try {
            transfer(cp.container.getInputStream(cp.path), destination.getStream(), true);
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
        } catch (StorageException e) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Figures out the best intent of the user for the destination filename to
     * use:
     * <ul>
     * <li>if a destination path is provided, use it (e.g. PUT source
     * destination or through a URI, LCOPY source router:host/destination).</li>
     * <li>if the destination path matches the host alias (e.g. LCOPY source
     * router:host), prefer the source filename</li>
     * <li>if the destination is not useful and the source is not empty, use
     * it</li>
     * 
     * @param put the {@link PutCommand}
     * @return a String to use as the filename
     */
    private String bestFilename(PutCommand put) {
        String destination = put.getDestination().getPath();
        if (Strings.isNullOrEmpty(destination) || destination.equals(getHost().getAlias())) {
            String source = put.getSource().getPath();
            if (!Strings.isNullOrEmpty(source)) {
                destination = source;
            }
        }
        return destination;
    }

    @Command(name = PUT, options = { Unique, Delete, Append })
    public ConnectorCommandResult put(PutCommand put) throws
            ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        String path = put.getDestination().getPath();
        IConnectorOutgoing source = put.getSource();
        String filename = bestFilename(put);

        logger.debug(String.format("PUT local '%s' to remote '%s' (matching filename '%s')", source.getPath(), path,
                filename));
        setup();
        ContainerAndPath cp = account.parse(container, path);

        if (cp.container == null) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        boolean unique = ConnectorCommandUtil.isOptionOn(put.getOptions(), Unique);
        boolean append = ConnectorCommandUtil.isOptionOn(put.getOptions(), Append);

        try {
            transfer(put.getSource().getStream(), cp.container.getOutputStream(cp.path, append, unique), false);
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
        } catch (StorageException e) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Get the file attribute view associated with a file path
     * 
     * @param path the file path
     * @return the file attributes
     * @throws com.cleo.connector.api.ConnectorException
     * @throws java.io.IOException
     * @throws StorageException 
     * @throws URISyntaxException 
     * @throws InvalidKeyException 
     */
    @Command(name = ATTR)
    public BasicFileAttributeView getAttributes(String path)
            throws ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        logger.debug(String.format("ATTR '%s'", path));
        setup();
        if (path.equals(".")) path = ""; // TODO: remove when Harmony is fixed
        ContainerAndPath cp = account.parse(container, path);

        if (cp.container == null) {
            return new BlobStorageEmptyAttributes(logger);
        } else if (Strings.isNullOrEmpty(cp.path)) {
            // return an Attr object representing the container
            return new BlobStorageContainerAttributes(cp.container.getProperties(), logger);
        } else {
            try {
                return new BlobStorageBlobAttributes(cp.container.getBlob(cp.path).getProperties(), logger);
            } catch (StorageException | URISyntaxException e) {
                if (cp.container.dir(cp.path).iterator().hasNext()) { // this is like isempty, but without skipping the placeholder
                    // use the container properties as the closest proxy for the
                    // virtual directory properties
                    return new BlobStorageContainerAttributes(cp.container.getProperties(), logger);
                } else {
                    throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                            ConnectorException.Category.fileNonExistentOrNoAccess);
                }
            }
        }
    }

    @Command(name = DELETE)
    public ConnectorCommandResult delete(OtherCommand delete) throws
            ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        String path = delete.getSource();
        logger.debug(String.format("DELETE '%s'", path));
        setup();
        ContainerAndPath cp = account.parse(container, path);

        if (cp.container == null) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        try {
            CloudBlob blob = cp.container.getBlob(cp.path);
            blob.delete();
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
        } catch (URISyntaxException | StorageException e) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }
    }

    @Command(name = MKDIR)
    public ConnectorCommandResult mkdir(OtherCommand mkdir) throws
            ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        String path = mkdir.getSource();
        logger.debug(String.format("MKDIR '%s'", path));
        setup();
        if (path.equals(".")) path = ""; // TODO: remove when Harmony is fixed
        ContainerAndPath cp = account.parse(container, path);

        if (cp.container == null) {
            throw new ConnectorException("MKDIR: directory name is required");
        } else if (Strings.isNullOrEmpty(cp.path)) {
            if (container == null) {
                // mkdir "container" attempt
                try {
                    cp.container.create();
                    return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
                } catch (StorageException e) {
                    throw new ConnectorException("MKDIR cannot create container "+path, e);
                }
            } else {
                // mkdir "/" attempt within a connection-defined container
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
            }
        }

        // regular mkdir request
        try {
            cp.container.mkdir(cp.path);
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
        } catch (URISyntaxException | StorageException e) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }
    }

    @Command(name = RMDIR)
    public ConnectorCommandResult rmdir(OtherCommand mkdir) throws
            ConnectorException, IOException, InvalidKeyException, URISyntaxException, StorageException {
        String path = mkdir.getSource();
        logger.debug(String.format("RMDIR '%s'", path));
        setup();
        if (path.equals(".")) path = ""; // TODO: remove when Harmony is fixed
        ContainerAndPath cp = account.parse(container, path);

        if (cp.container == null) {
            throw new ConnectorException("RMDIR: directory name is required");
        } else if (Strings.isNullOrEmpty(cp.path)) {
            if (container == null) {
                // rmdir "container" attempt
                try {
                    cp.container.delete();
                } catch (StorageException e) {
                    throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                            e,
                            ConnectorException.Category.fileNonExistentOrNoAccess);
                }
            } else {
                // rmdir "/" attempt within a connection-defined container
                throw new ConnectorException("RMDIR: cannot remove /");
            }
        }

        // regular rmdir request
        try {
            cp.container.rmdir(cp.path);
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
        } catch (IOException e) {
            // TODO: not sure what error to return in this situation (non-empty dir)
            throw new ConnectorException(String.format("'%s' is not empty", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } catch (URISyntaxException | StorageException e) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }
    }
}
