package com.cleo.labs.connector.testing;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import com.cleo.connector.api.interfaces.IConnectorFile;
import com.cleo.connector.api.interfaces.IConnectorOutgoing;
import com.google.common.base.Strings;

public class StringSource implements IConnectorOutgoing {
    public static final String lorem = "lorem ipsem dolor sit amitLorem ipsum dolor sit amet, consectetur adipiscing elit, "+
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud "+
            "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit "+
            "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, "+
            "sunt in culpa qui officia deserunt mollit anim id est laborum.";

    public static final String oneK = Strings.repeat("A", 80)+"\n"+
            Strings.repeat("B", 80)+"\n"+
            Strings.repeat("C", 80)+"\n"+
            Strings.repeat("D", 80)+"\n"+
            Strings.repeat("E", 80)+"\n"+
            Strings.repeat("F", 80)+"\n"+
            Strings.repeat("G", 80)+"\n"+
            Strings.repeat("H", 80)+"\n"+
            Strings.repeat("I", 80)+"\n"+
            Strings.repeat("J", 80)+"\n"+
            Strings.repeat("K", 80)+"\n"+
            Strings.repeat("L", 80)+"\n"+
            Strings.repeat("M", 51)+"\n";

    private String path;
    private String content;
    public StringSource(String path, String content) {
        this.path = path;
        this.content = content;
    }
    @Override
    public String getDefaultName() { return null; }
    @Override
    public IConnectorFile getFile() { return null; }
    @Override
    public Long getLength() { return null; }
    @Override
    public Map<String, String> getMetadata() { return null; }
    @Override
    public String getName() { return path.replaceFirst(".*/", ""); }
    @Override
    public String getPath() { return path; }
    @Override
    public IConnectorFile getSentboxCopy() { return null; }
    @Override
    public InputStream getStream() { return new ByteArrayInputStream(content.getBytes()); }
    @Override
    public String getTransferId() { return "transfer-id"; }
    @Override
    public boolean isFile() { return false; }
    @Override
    public boolean isForward() { return false; }
    @Override
    public boolean isStream() { return true; }
    @Override
    public void setForward(boolean arg0) { }
    @Override
    public void setMetadata(Map<String, String> arg0) { }
    @Override
    public IConnectorOutgoing setStream(InputStream arg0) { return null; }
    @Override
    public void setTransferId(String arg0) { }
}
