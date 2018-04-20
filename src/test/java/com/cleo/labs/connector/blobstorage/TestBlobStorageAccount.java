package com.cleo.labs.connector.blobstorage;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.command.ConnectorCommandName;
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

public class TestBlobStorageAccount {

    private static BlobStorageConnectorClient setupClient() {
        BlobStorageConnectorSchema blobSchema = new BlobStorageConnectorSchema();
        blobSchema.setup();
        IConnector connector = new TestConnector(System.err)
                .set("StorageAccountName", TestConfigValues.ACCOUNT)
                .set("AccessKey", TestConfigValues.KEY)
                .set(CommonProperty.EnableDebug.name(), Boolean.TRUE.toString());
        BlobStorageConnectorClient client = new BlobStorageConnectorClient(blobSchema);
        IConnectorHost connectorHost = new TestConnectorHost(client);
        client.setup(connector, blobSchema, connectorHost);

        return client;
    }

    @Test
    public void testTestConnectorHost() {
        BlobStorageConnectorClient client = setupClient();
        assertEquals("blob", client.getHost().getSchemeName());
        assertTrue(client.getHost().isSupported(ConnectorCommandName.DELETE));
        assertFalse(client.getHost().isSupported(ConnectorCommandName.CONNECT));
        assertFalse(client.getHost().isSupported("no such thing"));
        assertTrue(client.getHost().isSupported("DIR"));
        assertEquals(TestConfigValues.ACCOUNT, client.getHost().getPropertyValue("StorageAccountName").orElse(""));
        assertEquals("true", client.getHost().getPropertyValue(CommonProperty.EnableDebug.name()).orElse(""));
        assertEquals("core.windows.net", client.getHost().getPropertyValue("EndpointSuffix").orElse(""));
        assertTrue(client.getHost().getPropertyValue("Container").isPresent());
        assertTrue(client.getHost().getPropertyValue("Container").get().isEmpty());
        assertFalse(client.getHost().getPropertyValue("NoSuchProperty").isPresent());
    }

    @Test
    public void testCreateContainer() throws ConnectorException {
        BlobStorageConnectorClient client = setupClient();
        ConnectorCommandResult result;

        String container = "container-"+UUID.randomUUID().toString();
        // make a new container
        result = Commands.mkdir(container).go(client);
        assertEquals(Status.Success, result.getStatus());
        // make it again -- it's existing, but still should be ok
        result = Commands.mkdir(container).go(client);
        assertEquals(Status.Success, result.getStatus());
        // now delete it
        result = Commands.rmdir(container).go(client);
        assertEquals(Status.Success, result.getStatus());
        // delete it (non existing) should also be ok
        result = Commands.rmdir(container).go(client);
        assertEquals(Status.Success, result.getStatus());
    }

    @Test
    public void testRoundTrip() throws Exception {
        BlobStorageConnectorClient client = setupClient();
        ConnectorCommandResult result;

        result = Commands.dir("").go(client);
        assertEquals(Status.Success, result.getStatus());
        List<Entry> entries = result.getDirEntries().orElse(Collections.emptyList());
        for (Entry e : entries) {
            System.out.println(e);
        }
        assertFalse(entries.isEmpty());
        String container = entries.get(0).getPath();

        String random = UUID.randomUUID().toString();
        StringSource source = new StringSource(random, StringSource.lorem);
        StringCollector destination = new StringCollector().name(random);

        String path = container+"/"+random;
        result = Commands.put(source, path).go(client);
        assertEquals(Status.Success, result.getStatus());
        result = Commands.get(path, destination).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertEquals(StringSource.lorem, destination.toString());
        result = Commands.delete(path).go(client);
        assertEquals(Status.Success, result.getStatus());

        result = Commands.dir(container).go(client);
        assertEquals(Status.Success, result.getStatus());
        entries = result.getDirEntries().orElse(Collections.emptyList());
        assertFalse(entries.stream().anyMatch((e) -> e.getPath().equals(random)));
    }

}
