package com.cleo.labs.connector.testing;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.cleo.connector.api.ConnectorAuth;
import com.cleo.connector.api.ConnectorClient;
import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.annotations.Auth;
import com.cleo.connector.api.annotations.Client;
import com.cleo.connector.api.annotations.Command;
import com.cleo.connector.api.annotations.Connector;
import com.cleo.connector.api.annotations.Property;
import com.cleo.connector.api.command.ConnectorCommandName;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.connector.shell.interfaces.IConnectorHost;

public class TestConnectorHost implements IConnectorHost {

    private ConnectorClient client;
    private boolean shuttingDown;
    private Set<String> pendingCache;

    public TestConnectorHost(ConnectorClient client) {
        this.client = client;
        this.shuttingDown = false;
        this.pendingCache = new HashSet<>();
    }

    @Override
    public String getSchemeName() {
        Connector connectorAnnotation = client.getConnectorConfig().getClass().getAnnotation(Connector.class);
        if (connectorAnnotation != null) {
            return connectorAnnotation.scheme();
        }
        return null;
    }

    @Override
    public boolean isSupported(ConnectorCommandName command) {
        return isSupported(command.getCommandName());
    }

    @Override
    public boolean isSupported(String command) {
        for (Method method : client.getClass().getDeclaredMethods()) {
            Command commandAnnotation = method.getAnnotation(Command.class);
            if (commandAnnotation != null && commandAnnotation.name().getCommandName().equals(command)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasConnectorClient() {
        Client clientAnnotation = client.getConnectorConfig().getClass().getAnnotation(Client.class);
        return clientAnnotation != null;
    }

    @Override
    public boolean hasConnectorAuth() {
        Auth authAnnotation = client.getConnectorConfig().getClass().getAnnotation(Auth.class);
        return authAnnotation != null;
    }

    @Override
    public ConnectorAuth getConnectorAuth() throws ConnectorException {
        Auth authAnnotation = client.getConnectorConfig().getClass().getAnnotation(Auth.class);
        if (authAnnotation != null) {
            try {
                return authAnnotation.value().newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new ConnectorException(e);
            }
        }
        return null;
    }

    private IConnectorProperty<?> getProperty(String name) {
        for (Field field : client.getConnectorConfig().getClass().getDeclaredFields()) {
            Property propertyAnnotation = field.getAnnotation(Property.class);
            if (propertyAnnotation != null) {
                try {
                    field.setAccessible(true);
                    IConnectorProperty<?> property = (IConnectorProperty<?>)field.get(client.getConnectorConfig());
                    if (property.getName().equals(name)) {
                        return property;
                    }
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    // ignore it and keep looking
                }
            }
        }
        return null;
    }

    @Override
    public Optional<String> getPropertyValue(String key) {
        IConnectorProperty<?> property = getProperty(key);
        if (property != null) {
            try {
                return Optional.ofNullable(property.getValue(client).toString());
            } catch (ConnectorPropertyException e) {
                // fall through to empty
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean isConnectorProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public String getUID() {
        // not for testing
        return null;
    }

    @Override
    public String getAlias() {
        // not for testing
        return null;
    }

    @Override
    public String getVersion() {
        // not for testing
        return null;
    }

    @Override
    public Optional<String> getUnderlyingPath(ConnectorClient client, String path) {
        return Optional.empty();
    }

    @Override
    public Thread getForwardHandlerThread() {
        // not for testing
        return null;
    }

    @Override
    public void startForwardHandlerThread(String hostConnectorUID, String docDBConnectorUID) {
        // not for testing
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean _isShuttingDown() {
        return shuttingDown;
    }

    @Override
    public void _setShuttingDown(boolean shuttingDown) {
        this.shuttingDown = shuttingDown;
    }

    @Override
    public void addToPendingCache(String transferID) {
        pendingCache.add(transferID);
    }

    @Override
    public void removeFromPendingCache(String transferID) {
        pendingCache.remove(transferID);
    }

    @Override
    public long getPendingCacheSize() {
        return pendingCache.size();
    }

}
