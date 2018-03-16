package com.cleo.labs.connector.blobstorage;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.HashMap;

import com.cleo.connector.api.property.ConnectorPropertyException;
import com.google.common.base.Strings;
import com.microsoft.azure.storage.OperationContext;

/**
 * A configuration wrapper around a {@link BlobStorageConnectorClient}
 * instance and its {@link BlobStorageConnectorSchema}, exposing bean-like
 * getters for the schema properties converted to their usable forms:
 * <table border="1">
 *   <tr><th>Property</th><th>Stored As</th><th>Returned as</th></tr>
 *   <tr><td>Storage Account Name</td><td>String</td><td>String</td></tr>
 *   <tr><td>Access Key</td><td>String</td><td>String</td></tr>
 *   <tr><td>Endpoint Suffix</td><td>String (core.windows.net by default)</td><td>String</td>
 *   <tr><td>Connection String</td><td>computed</td><td>String</td></tr>
 *   <tr><td>Container</td><td>String</td><td>String</td></tr>
 * </table>
 */
public class BlobStorageConnectorConfig {
    private BlobStorageConnectorClient client;
    private BlobStorageConnectorSchema schema;

    /**
     * Constructs a configuration wrapper around a {@link BlobStorageConnectorClient}
     * instance and its {@link BlobStorageConnectorSchema}, exposing bean-like
     * getters for the schema properties converted to their usable forms.
     * @param client the BlobStorageConnectorClient
     * @param schema its BlobStorageConnectorSchema
     */
    public BlobStorageConnectorConfig(BlobStorageConnectorClient client, BlobStorageConnectorSchema schema) {
        this.client = client;
        this.schema = schema;
    }
 
    /**
     * Gets the Storage Account Name property.
     * @return the Storage Account Name
     * @throws ConnectorPropertyException
     */
    public String getStorageAccountName() throws ConnectorPropertyException {
        return schema.storageAccountName.getValue(client);
    }

    /**
     * Gets the Access Key property.
     * @return the Access Key
     * @throws ConnectorPropertyException
     */
    public String getAccessKey() throws ConnectorPropertyException {
        return schema.accessKey.getValue(client);
    }

    /**
     * Gets the Endpoint Suffix property.
     * @return the Endpoint Suffix
     * @throws ConnectorPropertyException
     */
    public String getEndpointSuffix() throws ConnectorPropertyException {
        return schema.endpointSuffix.getValue(client);
    }

    /**
     * Gets a computed Connection String.
     * @return a Connection String
     * @throws ConnectorPropertyException
     */
    public String getConnectionString() throws ConnectorPropertyException {
        return "DefaultEndpointsProtocol=https;"+
                "AccountName="+getStorageAccountName()+";"+
                "AccountKey="+getAccessKey()+";"+
                "EndpointSuffix="+getEndpointSuffix()+";";
    }

    /**
     * Gets the Container name.
     * @return the Container name
     * @throws ConnectorPropertyException
     */
    public String getContainer() throws ConnectorPropertyException {
        return schema.container.getValue(client);
    }

    public Proxy getProxy() throws ConnectorPropertyException {
        String address = schema.proxyAddress.getValue(client);
        if (Strings.isNullOrEmpty(address)) {
            return null;
        } else {
            int port = schema.proxyPort.getValue(client);
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(address, port));
        }
    }

    public HashMap<String,String> getHeaders() throws ConnectorPropertyException {
        return HeadersTableProperty.toHeaders(schema.headersTable.getValue(client));
    }

    public OperationContext getOperationContext() throws ConnectorPropertyException {
        OperationContext context = new OperationContext();
        context.setProxy(getProxy());
        context.setUserHeaders(getHeaders());
        return context;
    }
}
