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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.Pair;

public final class UnixFileAttributeManager extends FileAttributeManager {
    private static final Pattern ENTRY_PATTERN = Pattern.compile("^([^:]+):[^:]+:(\\d+):.*$");
    
    private static boolean canStatOwnerAndGroup(Path path) {
        try {
            Files.readAttributes(path, "unix:owner,group", LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (IOException | UnsupportedOperationException e) {
            return false;
        }
    }
    
    private static Map<Integer, String> getEntries(BufferedReader reader) throws IOException {
        Map<Integer, String> idToName = new HashMap<>();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                return idToName;
            }
            Matcher m = ENTRY_PATTERN.matcher(line);
            if (m.matches()) {
                String name = m.group(1);
                int id = Integer.parseInt(m.group(2));
                idToName.put(id, name);
            }
        }
    }
    
    private static Map<Integer, String> getNssEntries(String passwdOrGroup) throws IOException {
        Process p = new ProcessBuilder("getent", passwdOrGroup).start();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            return getEntries(r);
        }
    }
    
    private static Pair<Map<Integer, String>, Map<Integer, String>> getUserAndGroupCaches() throws IOException {
        Map<Integer, String> userIdToUserName;
        Map<Integer, String> groupIdToGroupName;
        try {
            if (isNssAvailable()) {
                if (!Environment.IS_FORK_ALLOWED) {
                    throw new IOException("fork has been disabled");
                }
                userIdToUserName = getNssEntries("passwd");
                groupIdToGroupName = getNssEntries("group");
            } else {
                userIdToUserName = readPasswdOrGroupFile("/etc/passwd");
                groupIdToGroupName = readPasswdOrGroupFile("/etc/group");
            }
        } catch (IOException e) {
            if (canStatOwnerAndGroup(Paths.get(Text.DOT))) {
                return null;
            } else {
                throw e;
            }
        }
        return new Pair<>(userIdToUserName, groupIdToGroupName);
    }
    
    private static Map<String, GroupPrincipal> groupPrincipalsOf(Collection<String> groupNames) {
        UserPrincipalLookupService service = FileSystems.getDefault().getUserPrincipalLookupService();
        Map<String, GroupPrincipal> res = new HashMap<>(groupNames.size());
        for (String groupName : groupNames) {
            try {
                GroupPrincipal principal = service.lookupPrincipalByGroupName(groupName);
                res.put(groupName, principal);
            } catch (UserPrincipalNotFoundException e) {
                // ignored
            } catch (IOException e) {
                return res;
            }
        }
        return res;
    }
    
    private static boolean isNssAvailable() {
        return Files.isReadable(Paths.get("/etc/nsswitch.conf")) && Environment.isExecutable("getent");
    }
    
    private static Map<Integer, String> readPasswdOrGroupFile(String passwdOrGroup) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(passwdOrGroup), Charset.defaultCharset())) {
            return getEntries(reader);
        }
    }
    
    private static Map<String, UserPrincipal> userPrincipalsOf(Collection<String> userNames) {
        UserPrincipalLookupService service = FileSystems.getDefault().getUserPrincipalLookupService();
        Map<String, UserPrincipal> res = new HashMap<>(userNames.size());
        for (String userName : userNames) {
            try {
                UserPrincipal principal = service.lookupPrincipalByName(userName);
                res.put(userName, principal);
            } catch (UserPrincipalNotFoundException e) {
                // ignored
            } catch (IOException e) {
                return res;
            }
        }
        return res;
    }
    
    private final Group defaultGroup;
    
    private final User defaultUser;
    
    private final Map<Integer, String> groupIdToGroupName;
    
    private final boolean cacheEnabled;
    
    private final Map<String, GroupPrincipal> nameToGroupPrincipal;
    
    private final Map<String, UserPrincipal> nameToUserPrincipal;
    
    private final Map<Integer, String> userIdToUserName;
    
    public UnixFileAttributeManager(User defaultUser, Group defaultGroup, boolean isPreserveUser, boolean isPreserveGroup) throws IOException {
        this.defaultUser = defaultUser;
        this.defaultGroup = defaultGroup;
        
        Pair<Map<Integer, String>, Map<Integer, String>> resOrNull = getUserAndGroupCaches();
        this.cacheEnabled = resOrNull != null;
        
        if (this.cacheEnabled) {
            this.userIdToUserName = resOrNull.getFirst();
            this.groupIdToGroupName = resOrNull.getSecond();
            if (isPreserveUser) {
                this.nameToUserPrincipal = userPrincipalsOf(this.userIdToUserName.values());
            } else {
                this.nameToUserPrincipal = Collections.emptyMap();
            }
            if (isPreserveGroup) {
                this.nameToGroupPrincipal = groupPrincipalsOf(this.groupIdToGroupName.values());
            } else {
                this.nameToGroupPrincipal = Collections.emptyMap();
            }
        } else {
            this.userIdToUserName = null;
            this.groupIdToGroupName = null;
            this.nameToUserPrincipal = null;
            this.nameToGroupPrincipal = null;
        }
    }
    
    private RsyncFileAttributes cachedStat(Path path) throws IOException {
        String toStat = "unix:mode,lastModifiedTime,size,uid,gid";
        Map<String, Object> attrs = Files.readAttributes(path, toStat, LinkOption.NOFOLLOW_LINKS);
        int mode = (int) attrs.get("mode");
        long mtime = ((FileTime) attrs.get("lastModifiedTime")).to(TimeUnit.SECONDS);
        long size = (long) attrs.get("size");
        int uid = (int) attrs.get("uid");
        int gid = (int) attrs.get("gid");
        String userName = this.userIdToUserName.getOrDefault(uid, this.defaultUser.getName());
        String groupName = this.groupIdToGroupName.getOrDefault(gid, this.defaultGroup.getName());
        User user = new User(userName, uid);
        Group group = new Group(groupName, gid);
        
        return new RsyncFileAttributes(mode, size, mtime, user, group);
    }
    
    private RsyncFileAttributes fullStat(Path path) throws IOException {
        String toStat = "unix:mode,lastModifiedTime,size,uid,gid,owner,group";
        Map<String, Object> attrs = Files.readAttributes(path, toStat, LinkOption.NOFOLLOW_LINKS);
        int mode = (int) attrs.get("mode");
        long mtime = ((FileTime) attrs.get("lastModifiedTime")).to(TimeUnit.SECONDS);
        long size = (long) attrs.get("size");
        int uid = (int) attrs.get("uid");
        int gid = (int) attrs.get("gid");
        String userName = ((UserPrincipal) attrs.get("owner")).getName();
        String groupName = ((GroupPrincipal) attrs.get("group")).getName();
        User user = new User(userName, uid);
        Group group = new Group(groupName, gid);
        
        return new RsyncFileAttributes(mode, size, mtime, user, group);
    }
    
    private GroupPrincipal getGroupPrincipalFrom(String groupName) throws IOException {
        try {
            if (this.cacheEnabled) {
                return this.nameToGroupPrincipal.get(groupName);
            }
            UserPrincipalLookupService service = FileSystems.getDefault().getUserPrincipalLookupService();
            return service.lookupPrincipalByGroupName(groupName);
        } catch (IOException | UnsupportedOperationException e) {
            return null;
        }
    }
    
    private UserPrincipal getUserPrincipalFrom(String userName) throws IOException {
        try {
            if (this.cacheEnabled) {
                return this.nameToUserPrincipal.get(userName);
            }
            UserPrincipalLookupService service = FileSystems.getDefault().getUserPrincipalLookupService();
            return service.lookupPrincipalByName(userName);
        } catch (IOException | UnsupportedOperationException e) {
            return null;
        }
    }
    
    @Override
    public void setFileMode(Path path, int mode, LinkOption... linkOption) throws IOException {
        Files.setAttribute(path, "unix:mode", mode, linkOption);
    }
    
    @Override
    public void setGroup(Path path, Group group, LinkOption... linkOption) throws IOException {
        GroupPrincipal principal = this.getGroupPrincipalFrom(group.getName());
        if (principal == null) {
            this.setGroupId(path, group.getId(), linkOption);
        }
        Files.setAttribute(path, "unix:group", principal, linkOption);
    }
    
    @Override
    public void setGroupId(Path path, int gid, LinkOption... linkOption) throws IOException {
        Files.setAttribute(path, "unix:gid", gid, linkOption);
    }
    
    @Override
    public void setOwner(Path path, User user, LinkOption... linkOption) throws IOException {
        UserPrincipal principal = this.getUserPrincipalFrom(user.getName());
        if (principal == null) {
            this.setUserId(path, user.getId(), linkOption);
        }
        Files.setAttribute(path, "unix:owner", principal, linkOption);
    }
    
    @Override
    public void setUserId(Path path, int uid, LinkOption... linkOption) throws IOException {
        Files.setAttribute(path, "unix:uid", uid, linkOption);
    }
    
    @Override
    public RsyncFileAttributes stat(Path path) throws IOException {
        if (this.cacheEnabled) {
            return this.cachedStat(path);
        }
        return this.fullStat(path);
    }
}
