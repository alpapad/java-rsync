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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.util.FileOps;

public final class PosixFileAttributeManager extends FileAttributeManager {
    private static Set<PosixFilePermission> modeToPosixFilePermissions(int mode) {
        Set<PosixFilePermission> result = new HashSet<>();
        if (FileOps.isUserReadable(mode)) {
            result.add(PosixFilePermission.OWNER_READ);
        }
        if (FileOps.isUserWritable(mode)) {
            result.add(PosixFilePermission.OWNER_WRITE);
        }
        if (FileOps.isUserExecutable(mode)) {
            result.add(PosixFilePermission.OWNER_EXECUTE);
        }
        if (FileOps.isGroupReadable(mode)) {
            result.add(PosixFilePermission.GROUP_READ);
        }
        if (FileOps.isGroupWritable(mode)) {
            result.add(PosixFilePermission.GROUP_WRITE);
        }
        if (FileOps.isGroupExecutable(mode)) {
            result.add(PosixFilePermission.GROUP_EXECUTE);
        }
        if (FileOps.isOtherReadable(mode)) {
            result.add(PosixFilePermission.OTHERS_READ);
        }
        if (FileOps.isOtherWritable(mode)) {
            result.add(PosixFilePermission.OTHERS_WRITE);
        }
        if (FileOps.isOtherExecutable(mode)) {
            result.add(PosixFilePermission.OTHERS_EXECUTE);
        }
        return result;
    }

    private static int toMode(PosixFileAttributes attrs) {
        int mode = 0;

        if (attrs.isDirectory()) {
            mode |= FileOps.S_IFDIR;
        } else if (attrs.isRegularFile()) {
            mode |= FileOps.S_IFREG;
        } else if (attrs.isSymbolicLink()) {
            mode |= FileOps.S_IFLNK;
        } else {
            mode |= FileOps.S_IFUNK;
        }

        Set<PosixFilePermission> perms = attrs.permissions();
        if (perms.contains(PosixFilePermission.OWNER_READ)) {
            mode |= FileOps.S_IRUSR;
        }
        if (perms.contains(PosixFilePermission.OWNER_WRITE)) {
            mode |= FileOps.S_IWUSR;
        }
        if (perms.contains(PosixFilePermission.OWNER_EXECUTE)) {
            mode |= FileOps.S_IXUSR;
        }
        if (perms.contains(PosixFilePermission.GROUP_READ)) {
            mode |= FileOps.S_IRGRP;
        }
        if (perms.contains(PosixFilePermission.GROUP_WRITE)) {
            mode |= FileOps.S_IWGRP;
        }
        if (perms.contains(PosixFilePermission.GROUP_EXECUTE)) {
            mode |= FileOps.S_IXGRP;
        }
        if (perms.contains(PosixFilePermission.OTHERS_READ)) {
            mode |= FileOps.S_IROTH;
        }
        if (perms.contains(PosixFilePermission.OTHERS_WRITE)) {
            mode |= FileOps.S_IWOTH;
        }
        if (perms.contains(PosixFilePermission.OTHERS_EXECUTE)) {
            mode |= FileOps.S_IXOTH;
        }

        return mode;
    }

    private final int defaultGroupId;
    private final int defaultUserId;

    private final Map<String, GroupPrincipal> nameToGroupPrincipal = new HashMap<>();

    private final Map<String, UserPrincipal> nameToUserPrincipal = new HashMap<>();

    public PosixFileAttributeManager(int defaultUserId, int defaultGroupId) {
        this.defaultUserId = defaultUserId;
        this.defaultGroupId = defaultGroupId;
    }

    private GroupPrincipal getGroupPrincipalFrom(String groupName) throws IOException {
        try {
            GroupPrincipal principal = nameToGroupPrincipal.get(groupName);
            if (principal == null) {
                UserPrincipalLookupService service = FileSystems.getDefault().getUserPrincipalLookupService();
                principal = service.lookupPrincipalByGroupName(groupName);
                nameToGroupPrincipal.put(groupName, principal);
            }
            return principal;
        } catch (UnsupportedOperationException e) {
            throw new IOException(e);
        }
    }

    private UserPrincipal getUserPrincipalFrom(String userName) throws IOException {
        try {
            UserPrincipal principal = nameToUserPrincipal.get(userName);
            if (principal == null) {
                UserPrincipalLookupService service = FileSystems.getDefault().getUserPrincipalLookupService();
                principal = service.lookupPrincipalByName(userName);
                nameToUserPrincipal.put(userName, principal);
            }
            return principal;
        } catch (UnsupportedOperationException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void setFileMode(Path path, int mode, LinkOption... linkOption) throws IOException {
        // i.e. (mode & 07000) != 0;
        boolean requiresUnix = (mode & (FileOps.S_ISUID | FileOps.S_ISGID | FileOps.S_ISVTX)) != 0;
        if (requiresUnix) {
            throw new IOException("unsupported operation");
        }
        Set<PosixFilePermission> perms = modeToPosixFilePermissions(mode);
        Files.setAttribute(path, "posix:permissions", perms, linkOption);
    }

    @Override
    public void setGroup(Path path, Group group, LinkOption... linkOption) throws IOException {
        GroupPrincipal principal = getGroupPrincipalFrom(group.getName());
        Files.setAttribute(path, "posix:group", principal, linkOption);
    }

    @Override
    public void setOwner(Path path, User user, LinkOption... linkOption) throws IOException {
        UserPrincipal principal = getUserPrincipalFrom(user.getName());
        Files.setAttribute(path, "posix:owner", principal, linkOption);
    }

    @Override
    public RsyncFileAttributes stat(Path path) throws IOException {
        PosixFileAttributes attrs = Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        UserPrincipal userPrincipal = attrs.owner();
        String userName = userPrincipal.getName();
        GroupPrincipal groupPrincipal = attrs.group();
        String groupName = groupPrincipal.getName();
        nameToUserPrincipal.putIfAbsent(userName, userPrincipal);
        nameToGroupPrincipal.putIfAbsent(groupName, groupPrincipal);
        return new RsyncFileAttributes(toMode(attrs), attrs.size(), attrs.lastModifiedTime().to(TimeUnit.SECONDS), new User(userName, defaultUserId), new Group(groupName, defaultGroupId));
    }
}
