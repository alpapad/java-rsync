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
    private final Group _group;
    private final long _lastModified;
    private final int _mode;
    private final long _size;
    private final User _user;
    
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
        this._mode = mode;
        this._size = fileSize;
        this._lastModified = lastModified;
        this._user = user;
        this._group = group;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj != null && this.getClass() == obj.getClass()) {
            RsyncFileAttributes other = (RsyncFileAttributes) obj;
            return this._lastModified == other._lastModified && this._size == other._size && this._mode == other._mode && this._user.equals(other._user) && this._group.equals(other._group);
            
        }
        return false;
    }
    
    public int fileType() {
        return FileOps.fileType(this._mode);
    }
    
    public Group group() {
        return this._group;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this._lastModified, this._size, this._mode, this._user, this._group);
    }
    
    public boolean isBlockDevice() {
        return FileOps.isBlockDevice(this._mode);
    }
    
    public boolean isCharacterDevice() {
        return FileOps.isCharacterDevice(this._mode);
    }
    
    public boolean isDirectory() {
        return FileOps.isDirectory(this._mode);
    }
    
    public boolean isFifo() {
        return FileOps.isFIFO(this._mode);
    }
    
    public boolean isOther() {
        return FileOps.isOther(this._mode);
    }
    
    public boolean isRegularFile() {
        return FileOps.isRegularFile(this._mode);
    }
    
    public boolean isSocket() {
        return FileOps.isSocket(this._mode);
    }
    
    public boolean isSymbolicLink() {
        return FileOps.isSymbolicLink(this._mode);
    }
    
    public long lastModifiedTime() {
        return this._lastModified;
    }
    
    public int mode() {
        return this._mode;
    }
    
    public long size() {
        return this._size;
    }
    
    @Override
    public String toString() {
        return String.format("%s (type=%s, mode=%#o, size=%d, " + "lastModified=%d, user=%s, group=%s)", this.getClass().getSimpleName(), FileOps.fileTypeToString(this._mode), this._mode, this._size,
                this._lastModified, this._user, this._group);
    }
    
    public User user() {
        return this._user;
    }
}
