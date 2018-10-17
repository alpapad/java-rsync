/*
 * Safe path resolving with a module root dir
 *
 * Copyright (C) 2014, 2016 Per Lundqvist
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
package com.github.perlundq.yajsync.server.module;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.RsyncSecurityException;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.PathOps;

/**
 * A RestrictedPath is a representation of a module and its root directory path
 * that provides robust semantics for safely resolving any untrusted path coming
 * from a possible external source. It allows resolving of any path that is
 * below the module root directory and will throw a RsyncSecurityException for
 * any other path.
 */
public final class RestrictedPath {
    private static final Pattern MODULE_REGEX = Pattern.compile("^\\w+$");
    private final Path dotDir;
    private final Path dotDotDir;
    private final String moduleName;
    private final Path rootPath;
    
    /**
     * @param moduleName
     * @param rootPath   the absolute path to the module top directory.
     */
    public RestrictedPath(String moduleName, Path rootPath) {
        if (!MODULE_REGEX.matcher(moduleName).matches()) {
            throw new IllegalArgumentException(String.format("rsync module must consist of alphanumeric characters " + "and underscore only: %s", moduleName));
        }
        assert rootPath.isAbsolute() : rootPath;
        this.moduleName = moduleName;
        this.rootPath = rootPath.normalize();
        this.dotDir = this.rootPath.getFileSystem().getPath(Text.DOT);
        this.dotDotDir = this.rootPath.getFileSystem().getPath(Text.DOT_DOT);
    }
    
    @Override
    public boolean equals(Object other) {
        if (other != null && other.getClass() == this.getClass()) {
            RestrictedPath otherPath = (RestrictedPath) other;
            return this.moduleName.equals(otherPath.moduleName) && this.rootPath.equals(otherPath.rootPath);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.moduleName, this.rootPath);
    }
    
    /**
     * resolve other in a secure manner without any call to stat.
     *
     * @throws RsyncSecurityException
     */
    private Path resolve(Path path) throws RsyncSecurityException {
        Path result;
        Path normalized = path.normalize();
        if (normalized.startsWith(this.moduleName)) {
            if (normalized.getNameCount() == 1) {
                result = this.rootPath;
            } else {
                Path strippedOfModulePrefix = normalized.subpath(1, normalized.getNameCount());
                result = this.rootPath.resolve(strippedOfModulePrefix).normalize();
            }
        } else {
            throw new RsyncSecurityException(String.format("\"%s\" is outside virtual dir for module %s", path, this.moduleName));
        }
        if (path.endsWith(this.dotDir)) {
            return result.resolve(this.dotDir);
        } else {
            return result;
        }
    }
    
    public Path resolve(String pathName) throws RsyncSecurityException {
        try {
            Path otherPath = PathOps.get(this.rootPath.getFileSystem(), pathName);
            Path resolved = this.resolve(otherPath);
            if (PathOps.contains(resolved, this.dotDotDir)) {
                throw new RsyncSecurityException(String.format("resolved path of %s contains ..: %s", pathName, resolved));
            }
            return resolved;
        } catch (InvalidPathException e) {
            throw new RsyncSecurityException(e);
        }
    }
    
    @Override
    public String toString() {
        return String.format("%s(name=%s, root=%s)", this.getClass().getSimpleName(), this.moduleName, this.rootPath);
    }
}
