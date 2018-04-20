package com.cleo.labs.connector.blobstorage;

import static com.cleo.connector.api.property.CommonPropertyGroups.Connect;
import static com.cleo.connector.api.property.CommonPropertyGroups.ConnectSecurity;

import java.io.IOException;

import com.cleo.connector.api.ConnectorConfig;
import com.cleo.connector.api.annotations.Client;
import com.cleo.connector.api.annotations.Connector;
import com.cleo.connector.api.annotations.Info;
import com.cleo.connector.api.annotations.Property;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.property.CommonProperties;
import com.cleo.connector.api.property.CommonProperty;
import com.cleo.connector.api.property.PropertyBuilder;
import com.cleo.connector.api.property.PropertyRange;
import com.cleo.connector.api.interfaces.IConnectorProperty.Attribute;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

@Connector(scheme = "blob", description = "Azure Blob Storage")
@Client(BlobStorageConnectorClient.class)
public class BlobStorageConnectorSchema extends ConnectorConfig {

    @Property
    final IConnectorProperty<String> storageAccountName = new PropertyBuilder<>("StorageAccountName", "")
            .setDescription("The Azure Storage account name.")
            .setGroup(Connect)
            .setRequired(true)
            .build();

    @Property
    final IConnectorProperty<String> accessKey = new PropertyBuilder<>("AccessKey", "")
            .setDescription("The Azure Storage account Access key.")
            .setGroup(Connect)
            .setRequired(true)
            .addAttribute(Attribute.Password)
            .build();

    @Property
    final IConnectorProperty<String> endpointSuffix = new PropertyBuilder<>("EndpointSuffix", "core.windows.net")
            .setDescription("The Azure endpoint suffix.")
            .setGroup(Connect)
            .setRequired(true)
            .build();

    @Property
    final IConnectorProperty<String> container = new PropertyBuilder<>("Container", "")
            .setDescription("The Azure Storage Container name.")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> proxyAddress = new PropertyBuilder<>("ProxyAddress", "")
            .setDescription("An optional HTTP proxy IP address or hostname.")
            .setGroup(ConnectSecurity)
            .setRequired(false)
            .build();

    @Property
    final IConnectorProperty<Integer> proxyPort = new PropertyBuilder<>("ProxyPort", 0)
            .setDescription("An optional HTTP proxy IP port.")
            .setGroup(ConnectSecurity)
            .setRequired(false)
            .setPossibleRanges(new PropertyRange<>(0,65535))
            .build();

    @Property
    final public IConnectorProperty<String> headersTable = new PropertyBuilder<>("Headers", "")
            .setRequired(false)
            .setGroup(ConnectSecurity)
            .setAllowedInSetCommand(false)
            .setDescription("A list of additional HTTP headers.")
            .setExtendedClass(HeadersTableProperty.class)
            .build();

    @Property
    final IConnectorProperty<Integer> commandRetries = CommonProperties.of(CommonProperty.CommandRetries);

    @Property
    final IConnectorProperty<Integer> commandRetryDelay = CommonProperties.of(CommonProperty.CommandRetryDelay);

    @Property
    final IConnectorProperty<Boolean> doNotSendZeroLengthFiles = CommonProperties.of(CommonProperty.DoNotSendZeroLengthFiles);

    @Property
    final IConnectorProperty<Boolean> deleteReceivedZeroLengthFiles = CommonProperties.of(CommonProperty.DeleteReceivedZeroLengthFiles);

    @Property
    final IConnectorProperty<String> retrieveDirectorySort = CommonProperties.of(CommonProperty.RetrieveDirectorySort);

    @Property
    final IConnectorProperty<Boolean> enableDebug = CommonProperties.of(CommonProperty.EnableDebug);

    @Info
    protected static String info() throws IOException {
        return Resources.toString(BlobStorageConnectorSchema.class.getResource("info.txt"), Charsets.UTF_8);
    }
}