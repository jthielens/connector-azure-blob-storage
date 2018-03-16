package com.cleo.labs.connector.blobstorage;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;

import com.cleo.connector.api.helper.Logger;
import com.microsoft.azure.storage.blob.BlobContainerProperties;

/**
 * Router file attribute views
 */
public class BlobStorageContainerAttributes implements DosFileAttributes, DosFileAttributeView {
    private BlobContainerProperties properties;
    private Logger logger;

    public BlobStorageContainerAttributes(BlobContainerProperties properties, Logger logger) {
        this.properties = properties;
        this.logger = logger;
    }

    @Override
    public FileTime lastModifiedTime() {
        Date time = properties.getLastModified();
        logger.debug(String.format("lastModifidedTime()=%s", time));
        if (time==null) {
            time = new Date();
        }
        return FileTime.fromMillis(time.getTime());
    }

    @Override
    public FileTime lastAccessTime() {
        Date time = properties.getLastModified();
        logger.debug(String.format("lastAccessTime()=%s", time));
        if (time==null) {
            time = new Date();
        }
        return FileTime.fromMillis(time.getTime());
    }

    @Override
    public FileTime creationTime() {
        Date time = properties.getLastModified();
        logger.debug(String.format("creationTime()=%s", time));
        if (time==null) {
            time = new Date();
        }
        return FileTime.fromMillis(time.getTime());
    }

    @Override
    public boolean isRegularFile() {
        logger.debug("isRegularFile()=false");
        return false; // containers are directories
    }

    @Override
    public boolean isDirectory() {
        logger.debug("isDirectory()=true");
        return true; // containers are directories
    }

    @Override
    public boolean isSymbolicLink() {
        logger.debug("isSymbolicLink()=false");
        return false; // containers are directories
    }

    @Override
    public boolean isOther() {
        logger.debug("isOther()=false");
        return false; // containers are directories
    }

    @Override
    public long size() {
        logger.debug("size()=0L");
        return 0L;
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
        return "container";
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
