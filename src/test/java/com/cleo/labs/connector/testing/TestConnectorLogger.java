package com.cleo.labs.connector.testing;

import java.io.PrintStream;
import java.util.Optional;

import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.interfaces.IConnectorIncoming;
import com.cleo.connector.api.interfaces.IConnectorOutgoing;
import com.cleo.connector.shell.interfaces.IConnectorLogger;

public class TestConnectorLogger implements IConnectorLogger {
    private PrintStream out;
    @Override
    public void logFile(IConnectorIncoming incoming, Entry entry, int fileNum, int fileCount) {
    }
    @Override
    public void logFile(IConnectorOutgoing outgoing, Entry entry, int fileNum, int fileCount) {
    }
    @Override
    public ConnectorCommandResult logResult(ConnectorCommandResult connectorCommandResult) {
        return connectorCommandResult;
    }
    @Override
    public ConnectorCommandResult logResult(ConnectorCommandResult connectorCommandResult, String copyPath) {
        return connectorCommandResult;
    }
    @Override
    public ConnectorCommandResult logResult(ConnectorCommandResult connectorCommandResult, long fileSize,
            long lastModified, String copyPath) {
        // TODO Auto-generated method stub
        return connectorCommandResult;
    }
    @Override
    public ConnectorCommandResult logResult(Status status, Optional<String> message) {
        return new ConnectorCommandResult(status);
    }
    @Override
    public ConnectorCommandResult logResult(Status status, Optional<String> message, String copyPath) {
        return new ConnectorCommandResult(status);
    }
    @Override
    public ConnectorCommandResult logResult(Status status, Optional<String> message, long fileSize,
            long lastModified, String copyPath) {
        return new ConnectorCommandResult(status);
    }
    @Override
    public void logException(Exception ex, boolean logResult) {
    }
    @Override
    public void logRequest(String requestType, String requestText) {
        out.println(String.format("%s: %s", requestType, requestText));
    }
    @Override
    public void logResponse(Integer responseCode, String responseText, String message, Status status) {
        out.println(String.format("%s (%s): %s - %s", responseText, responseCode, message, status.name()));
    }
    @Override
    public void logResponse(String responseLine, String message, Status status) {
        out.println(String.format("%s: %s - %s", responseLine, message, status.name()));
    }
    @Override
    public void logDetail(String message, int level) {
        out.println(String.format("%d: %s", level, message));
    }
    @Override
    public void logWarning(String message) {
        out.println(String.format("WARN: %s", message));
    }
    @Override
    public void logError(String message) {
        out.println(String.format("ERROR: %s", message));
    }
    @Override
    public void logThrowable(Throwable throwable) {
        out.println(String.format("EXCEPTION: %s", throwable.getMessage()));
    }
    @Override
    public void logHint(String content) {
        out.println(String.format("HINT: %s", content));
    }
    @Override
    public void debug(String message, Throwable throwable) {
        if (message!=null || throwable!=null) {
            if (message==null) {
                out.println(String.format("DEBUG: %s", throwable.getMessage()));
            } else if (throwable==null) {
                out.println(String.format("DEBUG: %s", message));
            } else {
                out.println(String.format("DEBUG: %s - %s", message, throwable.getMessage()));
            }
        }
    }
    public TestConnectorLogger(PrintStream out) {
        this.out = out;
    }
}
