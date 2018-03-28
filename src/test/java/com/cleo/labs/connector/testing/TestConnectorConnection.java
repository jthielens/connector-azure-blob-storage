package com.cleo.labs.connector.testing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.cleo.connector.api.helper.NetworkConnection;
import com.cleo.connector.shell.interfaces.IConnectorConnection;

public class TestConnectorConnection implements IConnectorConnection {

    public TestConnectorConnection() {
    }

    @Override
    public InputStream getConnectionInputStream(InputStream in) throws IOException {
        return in;
    }

    @Override
    public OutputStream getConnectionOutputStream(OutputStream out) throws IOException {
        return out;
    }

    @Override
    public void connect(NetworkConnection conn) throws IOException {
    }

}