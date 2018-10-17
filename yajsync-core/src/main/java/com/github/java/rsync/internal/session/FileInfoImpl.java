/*
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2016 Per Lundqvist
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
package com.github.java.rsync.internal.session;

import java.util.Arrays;

import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.internal.text.Text;

// NOTE: all internal FileInfo objects are instances of FileInfoImpl
class FileInfoImpl implements FileInfo {
    private static int cmp(byte a, byte b) {
        return (0xFF & a) - (0xFF & b);
    }

    /*
     * sort . (always a dir) before anything else sort files before dirs compare
     * dirs using a trailing slash
     */
    private static int compareUnixFileNamesBytes(byte[] leftBytes, boolean isLeftDir, byte[] rightBytes, boolean isRightDir) {
        if (isDotDir(leftBytes)) {
            if (isDotDir(rightBytes)) {
                return 0;
            }
            return -1;
        } else if (isDotDir(rightBytes)) {
            return 1;
        }

        if (isLeftDir != isRightDir) {
            if (isLeftDir) {
                return 1;
            }
            return -1;
        }

        int i = 0;
        for (; i < leftBytes.length && i < rightBytes.length; i++) {
            int diff = cmp(leftBytes[i], rightBytes[i]);
            if (diff != 0) {
                return diff;
            }
        }
        // one or both are at the end
        // one or both are a substring of the other
        // either both are directories or none is
        boolean isLeftAtEnd = i == leftBytes.length;
        boolean isRightAtEnd = i == rightBytes.length;

        if (isLeftDir) { // && isRightDir
            if (isLeftAtEnd && isRightAtEnd) {
                return 0;
            } else if (isLeftAtEnd) {
                return cmp(Text.ASCII_SLASH, rightBytes[i]);
            } else if (isRightAtEnd) {
                return cmp(leftBytes[i], Text.ASCII_SLASH);
            }
        }

        if (isLeftAtEnd == isRightAtEnd) {
            return 0;
        } else if (isLeftAtEnd) {
            return -1;
        }
        return 1;
    }

    private static boolean isDotDir(byte[] bytes) {
        return bytes.length == 1 && bytes[0] == Text.ASCII_DOT;
    }

    private final RsyncFileAttributes attrs;

    // pathName may only be null internally in Receiver, any such
    // instance will never be exposed externally
    private final String pathName;

    private final byte[] pathNameBytes;

    FileInfoImpl(String pathName, byte[] pathNameBytes, RsyncFileAttributes attrs) {
        assert pathNameBytes != null;
        assert attrs != null;
        assert pathNameBytes.length > 0;
        assert pathNameBytes[0] != Text.ASCII_SLASH;
        assert !isDotDir(pathNameBytes) || attrs.isDirectory();
        assert pathNameBytes[pathNameBytes.length - 1] != Text.ASCII_SLASH;

        this.pathName = pathName;
        this.pathNameBytes = pathNameBytes;
        this.attrs = attrs;
    }

    @Override
    public int compareTo(FileInfo otherFileInfo) {
        FileInfoImpl other = (FileInfoImpl) otherFileInfo;
        int result = compareUnixFileNamesBytes(pathNameBytes, attrs.isDirectory(), other.pathNameBytes, other.attrs.isDirectory());
        assert result != 0 || equals(other);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        // It is OK - FileInfo is not meant to be implemented by the API end
        // user and all our FileInfo implementing classes extends FileInfoImpl.
        if (obj instanceof FileInfoImpl) {
            FileInfoImpl other = (FileInfoImpl) obj;
            return Arrays.equals(pathNameBytes, other.pathNameBytes);
        } else {
            return false;
        }
    }

    @Override
    public RsyncFileAttributes getAttributes() {
        return attrs;
    }

    @Override
    public String getPathName() {
        return pathName;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(pathNameBytes);
    }

    boolean isDotDir() {
        return isDotDir(pathNameBytes);
    }

    @Override
    public String toString() {
        String str = pathName == null ? "untransferrable " + Text.bytesToString(pathNameBytes) : pathName;
        return String.format("%s (%s)", this.getClass().getSimpleName(), str);
    }
}
