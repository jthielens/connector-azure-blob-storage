package com.cleo.labs.connector.blobstorage;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;

import com.cleo.connector.api.helper.Logger;
import com.microsoft.azure.storage.blob.BlobProperties;

/**
 * Router file attribute views
 */
public class BlobStorageBlobAttributes implements DosFileAttributes, DosFileAttributeView {
    private BlobProperties properties;
    private Logger logger;

    public BlobStorageBlobAttributes(BlobProperties properties, Logger logger) {
        this.properties = properties;
        this.logger = logger;
    }

    @Override
    public FileTime lastModifiedTime() {
        logger.debug(String.format("lastModifiderTime()=%s", properties.getLastModified().toString()));
        return FileTime.fromMillis(properties.getLastModified().getTime());
    }

    @Override
    public FileTime lastAccessTime() {
        logger.debug(String.format("lastAccessTime()=%s", properties.getLastModified().toString()));
        return FileTime.fromMillis(properties.getLastModified().getTime());
    }

    @Override
    public FileTime creationTime() {
        logger.debug(String.format("creationTime()=%s", properties.getLastModified().toString()));
        return FileTime.fromMillis(properties.getLastModified().getTime());
    }

    @Override
    public boolean isRegularFile() {
        logger.debug("isRegularFile()=true");
        return true; // blobs are regular files
    }

    @Override
    public boolean isDirectory() {
        logger.debug("isDirectory()=false");
        return false; // blobs are regular files
    }

    @Override
    public boolean isSymbolicLink() {
        logger.debug("isSymbolicLink()=false");
        return false; // blobs are regular files
    }

    @Override
    public boolean isOther() {
        logger.debug("isOther()=false");
        return false; // blobs are regular files
    }

    @Override
    public long size() {
        logger.debug(String.format("size()=%d", properties.getLength()));
        return properties.getLength();
    }

    @Override
    public Object fileKey() {
        logger.debug("fileKey()=null");
        return null;
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported for Router");
        }
    }

    @Override
    public String name() {
        return "blob";
    }

    @Override
    public DosFileAttributes readAttributes() throws IOException {
        return this;
    }

    @Override
    public void setReadOnly(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported for Router");
    }

    @Override
    public void setHidden(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported for Router");
    }

    @Override
    public void setSystem(boolean value) throws IOException {
        throw new UnsupportedOperationException("setSystem() not supported for Router");
    }

    @Override
    public void setArchive(boolean value) throws IOException {
        throw new UnsupportedOperationException("setArchive() not supported for Router");
    }

    @Override
    public boolean isReadOnly() {
        logger.debug("isReadOnly()=false");
        return false;
    }

    @Override
    public boolean isHidden() {
        logger.debug("isHidden()=false");
        return false;
    }

    @Override
    public boolean isArchive() {
        logger.debug("isArchive()=false");
        return false;
    }

    @Override
    public boolean isSystem() {
        logger.debug("isSystem()=false");
        return false;
    }

}
