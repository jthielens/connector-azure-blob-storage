package com.cleo.labs.connector.blobstorage;

import static com.cleo.connector.api.command.ConnectorCommandOption.Append;
import static org.junit.Assert.*;

import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.property.CommonProperty;
import com.cleo.connector.shell.interfaces.IConnectorHost;
import com.cleo.labs.connector.testing.Commands;
import com.cleo.labs.connector.testing.StringCollector;
import com.cleo.labs.connector.testing.StringSource;
import com.cleo.labs.connector.testing.TestConnector;
import com.cleo.labs.connector.testing.TestConnectorHost;
import com.google.common.base.Strings;

public class TestBlobStorageConnectorClient {

    private static BlobStorageConnectorClient setupClient() {
        return setupClientAndContainer(TestConfigValues.CONTAINER);
    }

    private static BlobStorageConnectorClient setupClientAndContainer(String container) {
        BlobStorageConnectorSchema blobSchema = new BlobStorageConnectorSchema();
        blobSchema.setup();
        TestConnector connector = new TestConnector(System.err)
                .set("StorageAccountName", TestConfigValues.ACCOUNT)
                .set("AccessKey", TestConfigValues.KEY)
                .set(CommonProperty.EnableDebug.name(), Boolean.TRUE.toString());
        if (!Strings.isNullOrEmpty(container)) {
            connector.set("Container", container);
        }
        BlobStorageConnectorClient client = new BlobStorageConnectorClient(blobSchema);
        IConnectorHost connectorHost = new TestConnectorHost(client);
        client.setup(connector, blobSchema, connectorHost);

        return client;
    }

    @Test
    public void testDir() throws Exception {
        BlobStorageConnectorClient client = setupClientAndContainer(null);
        ConnectorCommandResult result;

        String randomContainer = "container-"+UUID.randomUUID().toString();
        // make a new container
        result = Commands.mkdir(randomContainer).go(client);
        assertEquals(Status.Success, result.getStatus());

        // get a handle on a container client
        BlobStorageConnectorClient container = setupClientAndContainer(randomContainer);

        // put a folder and file in the container
        result = Commands.mkdir("folder").go(container);
        assertEquals(Status.Success, result.getStatus());
        StringSource source;
        source = new StringSource("file1.txt", StringSource.lorem);
        result = Commands.put(source, "file1.txt").go(container);
        assertEquals(Status.Success, result.getStatus());
        source = new StringSource("file2.txt", StringSource.lorem);
        result = Commands.put(source, "folder/file2.txt").go(container);
        assertEquals(Status.Success, result.getStatus());

        // make sure prefixes are correct
        result = Commands.dir("").go(container);
        assertEquals(Status.Success, result.getStatus());
        for (Entry e : result.getDirEntries().orElse(Collections.emptyList())) {
            if (!e.getPath().equals("folder/")) {
                assertFalse("found / in "+e.getPath(), e.getPath().contains("/"));
            }
        }
        result = Commands.dir("folder").go(container);
        assertEquals(Status.Success, result.getStatus());
        for (Entry e : result.getDirEntries().orElse(Collections.emptyList())) {
            assertTrue("should start with folder/ in "+e.getPath(), e.getPath().startsWith("folder/"));
        }
        // repeat at the client level, now expecting a container prefix
        result = Commands.dir(randomContainer).go(client);
        assertEquals(Status.Success, result.getStatus());
        for (Entry e : result.getDirEntries().orElse(Collections.emptyList())) {
            assertTrue("should start with container/ in "+e.getPath(), e.getPath().startsWith(randomContainer+"/"));
        }
        result = Commands.dir(randomContainer+"/folder").go(client);
        assertEquals(Status.Success, result.getStatus());
        for (Entry e : result.getDirEntries().orElse(Collections.emptyList())) {
            assertTrue("should start with container/folder/ in "+e.getPath(), e.getPath().startsWith(randomContainer+"/folder/"));
        }

        // now delete the container
        result = Commands.rmdir(randomContainer).go(client);
        assertEquals(Status.Success, result.getStatus());
    }

    //@Ignore
    @Test
    public void testHomeDir() throws Exception {
        BlobStorageConnectorClient client = setupClient();
        ConnectorCommandResult result = Commands.dir("john").go(client);
        assertEquals(Status.Success, result.getStatus());
        List<Entry> entries = result.getDirEntries().orElse(Collections.emptyList());
        for (Entry e : entries) {
            System.out.println(e);
            BasicFileAttributeView attrs = client.getAttributes(e.getPath().replaceFirst("/$", ""));
            System.out.println("isFile()="+attrs.readAttributes().isRegularFile());
        }
        assertEquals(2, entries.size());
    }

    //@Ignore
    @Test
    public void testNewClient() throws Exception {
        BlobStorageConnectorClient client = setupClient();
        BasicFileAttributeView attrs = client.getAttributes("john");
        assertEquals(true, attrs.readAttributes().isDirectory());
    }

    @Test
    public void testRoundTrip() throws Exception {
        BlobStorageConnectorClient client = setupClient();
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
        BlobStorageConnectorClient client = setupClient();
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

        result = Commands.delete(append).go(client);
        assertEquals(Status.Success, result.getStatus());
    }
}
