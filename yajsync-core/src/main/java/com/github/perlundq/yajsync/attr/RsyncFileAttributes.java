/*
 * Rsync file attributes
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.perlundq.yajsync.attr;

import java.util.Objects;

import com.github.perlundq.yajsync.internal.util.FileOps;

public class RsyncFileAttributes {
    private final Group group;
    private final long lastModified;
    private final int mode;
    private final long size;
    private final User user;
    
    /**
     * @throws IllegalArgumentException if fileSize and/or lastModified is negative
     */
    public RsyncFileAttributes(int mode, long fileSize, long lastModified, User user, Group group) {
        assert user != null;
        assert group != null;
        if (fileSize < 0) {
            throw new IllegalArgumentException(String.format("illegal negative file size %d", fileSize));
        }
        if (lastModified < 0) {
            throw new IllegalArgumentException(String.format("illegal negative last modified time %d", lastModified));
        }
        this.mode = mode;
        this.size = fileSize;
        this.lastModified = lastModified;
        this.user = user;
        this.group = group;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj != null && this.getClass() == obj.getClass()) {
            RsyncFileAttributes other = (RsyncFileAttributes) obj;
            return this.lastModified == other.lastModified && this.size == other.size && this.mode == other.mode && this.user.equals(other.user) && this.group.equals(other.group);
            
        }
        return false;
    }
    
    public int getFileType() {
        return FileOps.fileType(this.mode);
    }
    
    public Group getGroup() {
        return this.group;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.lastModified, this.size, this.mode, this.user, this.group);
    }
    
    public boolean isBlockDevice() {
        return FileOps.isBlockDevice(this.mode);
    }
    
    public boolean isCharacterDevice() {
        return FileOps.isCharacterDevice(this.mode);
    }
    
    public boolean isDirectory() {
        return FileOps.isDirectory(this.mode);
    }
    
    public boolean isFifo() {
        return FileOps.isFIFO(this.mode);
    }
    
    public boolean isOther() {
        return FileOps.isOther(this.mode);
    }
    
    public boolean isRegularFile() {
        return FileOps.isRegularFile(this.mode);
    }
    
    public boolean isSocket() {
        return FileOps.isSocket(this.mode);
    }
    
    public boolean isSymbolicLink() {
        return FileOps.isSymbolicLink(this.mode);
    }
    
    public long lastModifiedTime() {
        return this.lastModified;
    }
    
    public int getMode() {
        return this.mode;
    }
    
    public long getSize() {
        return this.size;
    }
    
    @Override
    public String toString() {
        return String.format("%s (type=%s, mode=%#o, size=%d, " + "lastModified=%d, user=%s, group=%s)", this.getClass().getSimpleName(), FileOps.fileTypeToString(this.mode), this.mode, this.size,
                this.lastModified, this.user, this.group);
    }
    
    public User getUser() {
        return this.user;
    }
}
