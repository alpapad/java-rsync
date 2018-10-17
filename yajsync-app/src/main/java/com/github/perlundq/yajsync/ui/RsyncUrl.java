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
package com.github.perlundq.yajsync.ui;

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.PathOps;

final class RsyncUrl {
    private static final String HOST_REGEX = "[^:/]+";
    private static final String MODULE_REGEX = "[^/]+";
    private static final String PATH_REGEX = "/.*";
    private static final String PORT_REGEX = ":[0-9]+";
    /*
     * [USER@]HOST::SRC... rsync://[USER@]HOST[:PORT]/SRC
     */
    private static final String USER_REGEX = "[^@: ]+@";
    private static final Pattern MODULE = Pattern.compile(String.format("^(%s)?(%s)::(%s)?(%s)?$", USER_REGEX, HOST_REGEX, MODULE_REGEX, PATH_REGEX));
    private static final Pattern URL = Pattern.compile(String.format("^rsync://(%s)?(%s)(%s)?(/%s)?(%s)?$", USER_REGEX, HOST_REGEX, PORT_REGEX, MODULE_REGEX, PATH_REGEX));
    
    private static RsyncUrl local(Path cwd, String pathName) {
        try {
            return new RsyncUrl(cwd, null, "", pathName);
        } catch (IllegalUrlException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static RsyncUrl matchModule(Path cwd, String arg) throws IllegalUrlException {
        Matcher mod = MODULE.matcher(arg);
        if (!mod.matches()) {
            return null;
        }
        String userName = Text.stripLast(Text.nullToEmptyStr(mod.group(1)));
        String address = mod.group(2);
        String moduleName = Text.nullToEmptyStr(mod.group(3));
        String pathName = Text.nullToEmptyStr(mod.group(4));
        ConnectionInfo connInfo = new ConnectionInfo.Builder(address).userName(userName).build();
        return new RsyncUrl(cwd, connInfo, moduleName, pathName);
    }
    
    private static RsyncUrl matchUrl(Path cwd, String arg) throws IllegalUrlException {
        Matcher url = URL.matcher(arg);
        if (!url.matches()) {
            return null;
        }
        String userName = Text.stripLast(Text.nullToEmptyStr(url.group(1)));
        String address = url.group(2);
        String moduleName = Text.stripFirst(Text.nullToEmptyStr(url.group(4)));
        ConnectionInfo.Builder connInfoBuilder = new ConnectionInfo.Builder(address).userName(userName);
        if (url.group(3) != null) {
            int portNumber = Integer.parseInt(Text.stripFirst(url.group(3)));
            connInfoBuilder.portNumber(portNumber);
        }
        String pathName = Text.nullToEmptyStr(url.group(5));
        return new RsyncUrl(cwd, connInfoBuilder.build(), moduleName, pathName);
    }
    
    public static RsyncUrl parse(Path cwd, String arg) throws IllegalUrlException {
        assert arg != null;
        if (arg.isEmpty()) {
            throw new IllegalUrlException("empty string");
        }
        RsyncUrl result = matchModule(cwd, arg);
        if (result != null) {
            return result;
        }
        result = matchUrl(cwd, arg);
        if (result != null) {
            return result;
        }
        return RsyncUrl.local(cwd, arg);
    }
    
    private static String toRemotePathName(String pathName) {
        if (pathName.isEmpty()) {
            return Text.SLASH;
        } else {
            return pathName;
        }
    }
    
    private final ConnectionInfo connInfo;
    
    private final String moduleName;
    
    private final String pathName;
    
    public RsyncUrl(Path cwd, ConnectionInfo connInfo, String moduleName, String pathName) throws IllegalUrlException {
        assert pathName != null;
        assert moduleName != null;
        assert connInfo != null || moduleName.isEmpty() : connInfo + " " + moduleName;
        if (connInfo != null && moduleName.isEmpty() && !pathName.isEmpty()) {
            throw new IllegalUrlException(String.format("remote path %s specified without a module", pathName));
        }
        this.connInfo = connInfo;
        this.moduleName = moduleName;
        if (connInfo == null) {
            this.pathName = this.toLocalPathName(cwd, pathName);
        } else {
            this.pathName = toRemotePathName(pathName);
        }
    }
    
    public ConnectionInfo getConnectionInfo() {
        return this.connInfo;
    }
    
    public boolean isLocal() {
        return !this.isRemote();
    }
    
    public boolean isRemote() {
        return this.connInfo != null;
    }
    
    public String getModuleName() {
        return this.moduleName;
    }
    
    public String getPathName() {
        return this.pathName;
    }
    
    private String toLocalPathName(Path cwd, String pathName) {
        Path p = PathOps.get(cwd.getFileSystem(), pathName);
        return cwd.resolve(p).toString();
    }
    
    @Override
    public String toString() {
        if (this.connInfo != null) {
            return String.format("%s/%s%s", this.connInfo, this.moduleName, this.pathName);
        }
        return this.pathName;
    }
}
