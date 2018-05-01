package com.cleo.labs.connector.blobstorage;

import static com.cleo.connector.api.command.ConnectorCommandOption.Append;
import static org.junit.Assert.assertEquals;

import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.property.CommonProperty;
import com.cleo.connector.shell.interfaces.IConnector;
import com.cleo.connector.shell.interfaces.IConnectorHost;
import com.cleo.labs.connector.testing.Commands;
import com.cleo.labs.connector.testing.StringCollector;
import com.cleo.labs.connector.testing.StringSource;
import com.cleo.labs.connector.testing.TestConnector;
import com.cleo.labs.connector.testing.TestConnectorHost;

public class TestBlobStorageConnectorClient {

    private static BlobStorageConnectorClient setupClient() {
        BlobStorageConnectorSchema blobSchema = new BlobStorageConnectorSchema();
        blobSchema.setup();
        IConnector connector = new TestConnector(System.err)
                .set("StorageAccountName", TestConfigValues.ACCOUNT)
                .set("AccessKey", TestConfigValues.KEY)
                .set("Container", TestConfigValues.CONTAINER)
                .set(CommonProperty.EnableDebug.name(), Boolean.TRUE.toString());
        BlobStorageConnectorClient client = new BlobStorageConnectorClient(blobSchema);
        IConnectorHost connectorHost = new TestConnectorHost(client);
        client.setup(connector, blobSchema, connectorHost);

        return client;
    }

    @Ignore
    @Test
    public void testDir() throws Exception {
        BlobStorageConnectorClient client = setupClient();
        ConnectorCommandResult result = Commands.dir("folder-a").go(client);
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
