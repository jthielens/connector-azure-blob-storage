package com.cleo.labs.connector.testing;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Map;

import com.cleo.connector.api.interfaces.IConnectorFile;
import com.cleo.connector.api.interfaces.IConnectorIncoming;
import com.google.common.base.Charsets;

public class StringCollector implements IConnectorIncoming {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String transferId = null;
    String name = null;

    public StringCollector name(String name) {
        this.name = name;
        return this;
    }
    public String name() {
        return name;
    }

    public byte[] output() {
        return out.toByteArray();
    }
    @Override
    public String toString() {
        return new String(output(), Charsets.UTF_8);
    }

    @Override
    public IConnectorIncoming setFile(IConnectorFile file) {
        throw new UnsupportedOperationException();
    }
    @Override
    public IConnectorIncoming setStream(OutputStream stream) {
        throw new UnsupportedOperationException();
    }
    @Override
    public boolean isFile() {
        return false;
    }
    @Override
    public IConnectorFile getFile() {
        return null;
    }
    @Override
    public boolean isStream() {
        return true;
    }
    @Override
    public OutputStream getStream() {
        return out;
    }
    @Override
    public void setTransferId(String transferId) {
        this.transferId = transferId;
    }
    @Override
    public String getTransferId() {
        return transferId;
    }
    @Override
    public String getName() {
        return name;
    }
    @Override
    public String getPath() {
        return name;
    }
    @Override
    public IConnectorFile getReceivedboxCopy() {
        return null;
    }
    @Override
    public void setMetadata(Map<String, String> metadata) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Map<String, String> getMetadata() {
        return null;
    }
}
