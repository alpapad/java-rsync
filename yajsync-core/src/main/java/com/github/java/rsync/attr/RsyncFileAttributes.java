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
package com.github.java.rsync.attr;

import java.util.Objects;

import com.github.java.rsync.internal.util.FileOps;

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
        size = fileSize;
        this.lastModified = lastModified;
        this.user = user;
        this.group = group;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && this.getClass() == obj.getClass()) {
            RsyncFileAttributes other = (RsyncFileAttributes) obj;
            return lastModified == other.lastModified && size == other.size && mode == other.mode && user.equals(other.user) && group.equals(other.group);

        }
        return false;
    }

    public int getFileType() {
        return FileOps.fileType(mode);
    }

    public Group getGroup() {
        return group;
    }

    public int getMode() {
        return mode;
    }

    public long getSize() {
        return size;
    }

    public User getUser() {
        return user;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastModified, size, mode, user, group);
    }

    public boolean isBlockDevice() {
        return FileOps.isBlockDevice(mode);
    }

    public boolean isCharacterDevice() {
        return FileOps.isCharacterDevice(mode);
    }

    public boolean isDirectory() {
        return FileOps.isDirectory(mode);
    }

    public boolean isFifo() {
        return FileOps.isFIFO(mode);
    }

    public boolean isOther() {
        return FileOps.isOther(mode);
    }

    public boolean isRegularFile() {
        return FileOps.isRegularFile(mode);
    }

    public boolean isSocket() {
        return FileOps.isSocket(mode);
    }

    public boolean isSymbolicLink() {
        return FileOps.isSymbolicLink(mode);
    }

    public long lastModifiedTime() {
        return lastModified;
    }

    @Override
    public String toString() {
        return String.format("%s (type=%s, mode=%#o, size=%d, " + "lastModified=%d, user=%s, group=%s)", this.getClass().getSimpleName(), FileOps.fileTypeToString(mode), mode, size, lastModified,
                user, group);
    }
}
