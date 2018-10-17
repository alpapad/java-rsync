/*
 * Copyright (C) 2013-2015 Per Lundqvist
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
package com.github.java.rsync.ui;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

final class RsyncUrls {
    private final ConnectionInfo connInfo;
    private final String moduleName;
    private final Iterable<String> pathNames;
    
    public RsyncUrls(ConnectionInfo connInfo, String moduleName, Iterable<String> pathNames) {
        this.connInfo = connInfo;
        this.moduleName = moduleName;
        this.pathNames = pathNames;
    }
    
    public RsyncUrls(Path cwd, Iterable<String> urls) throws IllegalUrlException {
        List<String> pathNames = new LinkedList<>();
        RsyncUrl prevUrl = null;
        String moduleName = null;
        for (String s : urls) {
            RsyncUrl url = RsyncUrl.parse(cwd, s);
            boolean isFirst = prevUrl == null;
            boolean curAndPrevAreLocal = !isFirst && url.isLocal() && prevUrl.isLocal();
            boolean curAndPrevIsSameRemote = !isFirst && url.isRemote() && prevUrl.isRemote() && url.getConnectionInfo().equals(prevUrl.getConnectionInfo())
                    && url.getModuleName().equals(prevUrl.getModuleName());
            if (isFirst || curAndPrevAreLocal || curAndPrevIsSameRemote) {
                if (moduleName == null && url.isRemote()) {
                    moduleName = url.getModuleName();
                }
                if (!url.getPathName().isEmpty()) {
                    pathNames.add(url.getPathName());
                }
                prevUrl = url;
            } else {
                throw new IllegalUrlException(String.format("remote source arguments %s and %s are incompatible", prevUrl, url));
            }
        }
        if (prevUrl == null) {
            throw new IllegalArgumentException("empty sequence: " + urls);
        }
        this.pathNames = pathNames;
        this.moduleName = moduleName;
        connInfo = prevUrl.getConnectionInfo();
    }
    
    public ConnectionInfo getConnectionInfo() {
        return connInfo;
    }
    
    public String getModuleName() {
        return moduleName;
    }
    
    public Iterable<String> getPathNames() {
        return pathNames;
    }
    
    public boolean isRemote() {
        return connInfo != null;
    }
    
    @Override
    public String toString() {
        if (isRemote()) {
            return String.format("%s/%s%s", connInfo, moduleName, pathNames.toString());
        }
        return pathNames.toString();
    }
}
