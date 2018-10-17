/*
 * Properties utility routines
 *
 * Copyright (C) 2013, 2014, 2016 Per Lundqvist
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
package com.github.perlundq.yajsync.internal.util;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.User;

public final class Environment {
    
    private static final String ENV_RSYNC_PASSWORD = "RSYNC_PASSWORD";
    public static final boolean IS_FORK_ALLOWED = isForkAllowed();
    public static final boolean IS_RUNNING_WINDOWS = isRunningWindows();
    private static final String PROPERTY_KEY_ALLOCATE_DIRECT = "allocate.direct"; // not present unless manually defined
    private static final String PROPERTY_KEY_ALLOW_FORK = "allow.fork";
    private static final String PROPERTY_KEY_CWD = "user.dir";
    private static final String PROPERTY_KEY_GROUP_NAME = "user.group";
    private static final String PROPERTY_KEY_GROUP_UID = "user.gid"; // not present unless manually defined
    private static final String PROPERTY_KEY_UMASK = "umask";
    private static final String PROPERTY_KEY_USER_NAME = "user.name";
    
    private static final String PROPERTY_KEY_USER_UID = "user.uid"; // not present unless manually defined
    private static final String PROPERTY_OS_NAME = "os.name";
    private static final String PROPERTY_SERVER_CONFIG = "rsync.cfg";
    public static final int UMASK = umask();
    private static final String WINDOWS_NAME = "Windows";
    public static final int DEFAULT_DIR_PERMS = 0777 & ~UMASK;
    public static final int DEFAULT_FILE_PERMS = 0666 & ~UMASK;
    
    public static int getGroupId() {
        String gidString = System.getProperty(PROPERTY_KEY_GROUP_UID);
        if (gidString == null) {
            return Group.NOBODY.getId();
        }
        int gid = Integer.parseInt(gidString);
        if (gid < 0 || gid > Group.ID_MAX) {
            return Group.NOBODY.getId();
        }
        return gid;
        
    }
    
    public static String getGroupName() {
        return getPropertyOrDefault(PROPERTY_KEY_GROUP_NAME, Group.NOBODY.getName());
    }
    
    private static String getNonNullProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new RuntimeException("missing value for property with key " + key);
        }
        return value;
    }
    
    private static String getPropertyOrDefault(String key, String defaultValue) {
        return Util.defaultIfNull(System.getProperty(key), defaultValue);
    }
    
    public static String getRsyncPasswordOrNull() {
        return System.getenv(ENV_RSYNC_PASSWORD);
    }
    
    public static String getServerConfig(String defName) {
        return getPropertyOrDefault(PROPERTY_SERVER_CONFIG, defName);
    }
    
    public static int getUserId() {
        String uidString = System.getProperty(PROPERTY_KEY_USER_UID);
        if (uidString == null) {
            return User.NOBODY.getId();
        }
        int uid = Integer.parseInt(uidString);
        if (uid < 0 || uid > User.ID_MAX) {
            return User.NOBODY.getId();
        }
        return uid;
    }
    
    public static String getUserName() {
        return getPropertyOrDefault(PROPERTY_KEY_USER_NAME, User.NOBODY.getName());
    }
    
    public static String getWorkingDirectoryName() {
        return getNonNullProperty(PROPERTY_KEY_CWD);
    }
    
    public static boolean hasAllocateDirectArray() {
        return ByteBuffer.allocateDirect(1).hasArray();
    }
    
    public static boolean isAllocateDirect() {
        String value = Util.defaultIfNull(System.getProperty(PROPERTY_KEY_ALLOCATE_DIRECT), "true");
        return Boolean.valueOf(value);
    }
    
    public static boolean isExecutable(String commandName) {
        for (String dirName : System.getenv("PATH").split(":")) {
            Path p = Paths.get(dirName).resolve(commandName);
            if (Files.isRegularFile(p) && Files.isExecutable(p)) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean isForkAllowed() {
        String value = Util.defaultIfNull(System.getProperty(PROPERTY_KEY_ALLOW_FORK), "true");
        return Boolean.valueOf(value);
    }
    
    private static boolean isRunningWindows() {
        String osName = getNonNullProperty(PROPERTY_OS_NAME);
        return osName.startsWith(WINDOWS_NAME);
    }
    
    public static void setAllocateDirect(boolean isAllocateDirect) {
        System.setProperty(PROPERTY_KEY_ALLOCATE_DIRECT, Boolean.toString(isAllocateDirect));
    }
    
    private static int umask() {
        String value = System.getProperty(PROPERTY_KEY_UMASK);
        if (value == null) {
            return Consts.DEFAULT_UMASK;
        }
        return Integer.parseInt(value, 8);
    }
    
    private Environment() {
    }
}
