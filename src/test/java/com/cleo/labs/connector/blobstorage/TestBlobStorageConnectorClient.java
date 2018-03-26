package com.cleo.labs.connector.blobstorage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static com.cleo.connector.api.command.ConnectorCommandOption.Append;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import com.cleo.connector.api.ConnectorClient;
import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.helper.NetworkConnection;
import com.cleo.connector.api.interfaces.IConnectorConfig;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.property.CommonProperty;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.connector.shell.interfaces.IConnector;
import com.cleo.connector.shell.interfaces.IConnectorAction;
import com.cleo.connector.shell.interfaces.IConnectorConnection;
import com.cleo.connector.shell.interfaces.IConnectorHost;
import com.cleo.labs.connector.testing.Commands;
import com.cleo.labs.connector.testing.StringCollector;
import com.cleo.labs.connector.testing.StringSource;
import com.cleo.labs.connector.testing.TestConnectorLogger;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class TestBlobStorageConnectorClient {

    public static class TestConfig extends BlobStorageConnectorConfig {
        // config data
        private String storageAccountName;
        private String accessKey;
        private String container;
        // fluent setters
        public TestConfig storageAccountName(String storageAccountName) {
            this.storageAccountName = storageAccountName;
            return this;
        }
        public TestConfig accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }
        public TestConfig container(String container) {
            this.container = container;
            return this;
        }
        // override classic getters
        @Override
        public String getStorageAccountName() throws ConnectorPropertyException {
            return storageAccountName;
        }
        @Override
        public String getAccessKey() throws ConnectorPropertyException {
            return accessKey;
        }
        @Override
        public String getContainer() throws ConnectorPropertyException {
            return container;
        }
        @Override
        public String getEndpointSuffix() throws ConnectorPropertyException {
            return "core.windows.net";
        }
        @Override
        public Proxy getProxy() throws ConnectorPropertyException {
            return null;
        }
        @Override
        public HashMap<String, String> getHeaders() throws ConnectorPropertyException {
            return new HashMap<>();
        }
        // default constructor
        public TestConfig() {
            super(null, null);
        }
    }

    private static BlobStorageConnectorClient setupClient(BlobStorageConnectorClient client) {
        IConnectorConnection cc = new IConnectorConnection() {
            @Override
            public OutputStream getConnectionOutputStream(OutputStream os) throws IOException { return os; }
            @Override
            public InputStream getConnectionInputStream(InputStream is) throws IOException { return is; }
            @Override
            public void connect(NetworkConnection arg0) throws IOException { }
        };
        IConnectorAction ca = mock(IConnectorAction.class);
        when(ca.isInterrupted()).thenReturn(false);
        IConnector connector = mock(IConnector.class);
        when(connector.getConnectorConnection()).thenReturn(cc);
        when(connector.getConnectorAction()).thenReturn(ca);
        when(connector.getConnectorLogger()).thenReturn(new TestConnectorLogger(System.err));
        IConnectorConfig connectorConfig = mock(IConnectorConfig.class);
        @SuppressWarnings("unchecked")
        IConnectorProperty<Boolean> debugOnProperty = mock(IConnectorProperty.class);
        try {
            when(debugOnProperty.getValue(any(ConnectorClient.class))).thenReturn(Boolean.TRUE);
        } catch (ConnectorPropertyException ignore) {
            ignore.printStackTrace();
        }
        when(connectorConfig.getConnectorProperties()).thenReturn(Collections.singletonMap(CommonProperty.EnableDebug.name(), debugOnProperty));
        IConnectorHost connectorHost = mock(IConnectorHost.class);
        client.setup(connector, connectorConfig, connectorHost);

        return client;
    }

    private enum What {BLOB, DIR, NOTEXIST};
    @SuppressWarnings("unused")
    private static What whatisit(BlobStorageContainer container, String it) {
        try {
            container.getBlob(it); // just to see if it throws
            return What.BLOB;
        } catch (StorageException | URISyntaxException e) {
            boolean isdir = !container.isempty(it);
            if (isdir) {
                return What.DIR;
            } else {
                return What.NOTEXIST;
            }
        }
    }

    private static BlobStorageConnectorConfig CONFIG = new TestConfig()
                .storageAccountName(TestConfigValues.ACCOUNT)
                .accessKey(TestConfigValues.KEY)
                .container(TestConfigValues.CONTAINER);

    //@Ignore
    @Test
    public void testOne() throws Exception {
        BlobStorageContainer container = new BlobStorageContainer(new BlobStorageAccount(CONFIG), CONFIG.getContainer());

        /*
        System.out.println("mkdir(john)");
        container.mkdir("john");
        System.out.println("mkdir(john/inbox)");
        container.mkdir("john/inbox");
        System.out.println("mkdir(john/outbox)");
        container.mkdir("john/outbox");
        System.out.println("--------------------");
        */
        List<String> prefixes = new ArrayList<>();
        prefixes.add("");
        while (!prefixes.isEmpty()) {
            String prefix = prefixes.remove(0);
            for (ListBlobItem item : container.dir(prefix)) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory dir = (CloudBlobDirectory) item;
                    System.out.println(String.format("prefix=%s dir(%s)", prefix, dir.getPrefix()));
                    prefixes.add(dir.getPrefix());
                } else if (item instanceof CloudBlob) {
                    if (item instanceof CloudBlockBlob) {
                        CloudBlockBlob block = (CloudBlockBlob) item;
                        System.out.println(String.format("prefix=%s block(%s)", prefix, block.getName()));
                    } else if (item instanceof CloudPageBlob) {
                        CloudPageBlob page = (CloudPageBlob) item;
                        System.out.println(String.format("prefix=%s page(%s)", prefix, page.getName()));
                    } else if (item instanceof CloudAppendBlob) {
                        CloudAppendBlob append = (CloudAppendBlob) item;
                        System.out.println(String.format("prefix=%s page(%s)", prefix, append.getName()));
                    } else {
                        System.out.println(String.format("prefix=%s blob(%s)", prefix, item.toString()));
                    }
                } else {
                    System.out.println(String.format("prefix=%s item(%s)", prefix, item.toString()));
                }
            }
        }
        System.out.println("--------------------");
        /*
        assertEquals(What.DIR, whatisit(container, "folder-a"));
        assertEquals(What.DIR, whatisit(container, "folder-a/folder-b"));
        assertEquals(What.BLOB, whatisit(container, "test1.txt"));
        assertEquals(What.NOTEXIST, whatisit(container, "test1.txt/"));
        assertEquals(What.NOTEXIST, whatisit(container, "foobar"));
        */
    }

    @Ignore
    @Test
    public void testDir() throws Exception {
        BlobStorageConnectorClient client = setupClient(new BlobStorageConnectorClient(CONFIG));
        ConnectorCommandResult result = Commands.dir("").go(client);
        assertEquals(Status.Success, result.getStatus());
        List<Entry> entries = result.getDirEntries().orElse(Collections.emptyList());
        for (Entry e : entries) {
            System.out.println(e);
        }
        assertEquals(8, entries.size());
    }

    //@Ignore
    @Test
    public void testHomeDir() throws Exception {
        BlobStorageConnectorClient client = setupClient(new BlobStorageConnectorClient(CONFIG));
        ConnectorCommandResult result = Commands.dir("john").go(client);
        assertEquals(Status.Success, result.getStatus());
        List<Entry> entries = result.getDirEntries().orElse(Collections.emptyList());
        for (Entry e : entries) {
            System.out.println(e);
            BasicFileAttributeView attrs = client.getAttributes(e.getPath().replaceFirst("/$", ""));
            System.out.println(attrs);
        }
        assertEquals(2, entries.size());
    }

    //@Ignore
    @Test
    public void testNewClient() throws Exception {
        BlobStorageConnectorClient client = setupClient(new BlobStorageConnectorClient(CONFIG));
        BasicFileAttributeView attrs = client.getAttributes("john");
        assertEquals(true, attrs.readAttributes().isDirectory());
    }

    @Test
    public void testRoundTrip() throws Exception {
        BlobStorageConnectorClient client = setupClient(new BlobStorageConnectorClient(CONFIG));
        String random = UUID.randomUUID().toString();
        StringSource source = new StringSource(random, StringSource.lorem);
        StringCollector destination = new StringCollector().name(random);

        ConnectorCommandResult result = Commands.put(source, random).go(client);
        assertEquals(Status.Success, result.getStatus());
        result = Commands.get(random, destination).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertEquals(StringSource.lorem, destination.toString());
        result = Commands.delete(random).go(client);
        assertEquals(Status.Success, result.getStatus());
    }

    @Test
    public void testGetAppend() throws Exception {
        BlobStorageConnectorClient client = setupClient(new BlobStorageConnectorClient(CONFIG));
        String append = UUID.randomUUID().toString();
        StringSource source = new StringSource(append, "aabb");
        StringCollector destination = new StringCollector().name(append);
        ConnectorCommandResult result;
        
        result = Commands.put(source, append).option(Append).go(client);
        assertEquals(Status.Success, result.getStatus());
        result = Commands.get(append, destination).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertEquals("aabb", destination.toString());

        source = new StringSource(append, "ccdd");
        destination = new StringCollector().name(append);
        result = Commands.put(source, append).option(Append).go(client);
        assertEquals(Status.Success, result.getStatus());
        result = Commands.get(append, destination).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertEquals("aabbccdd", destination.toString());
    }
}
