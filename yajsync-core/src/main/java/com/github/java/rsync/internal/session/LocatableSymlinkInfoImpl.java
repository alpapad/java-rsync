/*
 * Copyright (C) 2016 Per Lundqvist
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

import java.nio.file.Path;

import com.github.java.rsync.attr.LocatableSymlinkInfo;
import com.github.java.rsync.attr.RsyncFileAttributes;

class LocatableSymlinkInfoImpl extends SymlinkInfoImpl implements LocatableSymlinkInfo {
    private final Path path;
    
    LocatableSymlinkInfoImpl(String pathName, byte[] pathNameBytes, RsyncFileAttributes attrs, String targetPathName, Path path) {
        super(pathName, pathNameBytes, attrs, targetPathName);
        assert path != null;
        assert path.isAbsolute();
        this.path = path;
    }
    
    @Override
    public Path getPath() {
        return this.path;
    }
}
