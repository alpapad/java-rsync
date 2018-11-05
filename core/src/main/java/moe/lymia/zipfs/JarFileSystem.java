/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package moe.lymia.zipfs;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

/**
 * Adds aliasing to ZipFileSystem to support multi-release jar files. An alias
 * map is created by {@link JarFileSystem#createVersionedLinks(int)}. The map is
 * then consulted when an entry is looked up in
 * {@link JarFileSystem#getEntry(byte[])} to determine if the entry has a
 * corresponding versioned entry. If so, the versioned entry is returned.
 *
 * @author Steve Drach
 */

class JarFileSystem extends ZipFileSystem {
    private Function<byte[], byte[]> lookup;

    JarFileSystem(ZipFileSystemProvider provider, Path zfpath, Map<String, ?> env) throws IOException {
        super(provider, zfpath, env);
        lookup = path -> path; // lookup needs to be set before isMultiReleaseJar is called
                               // because it eventually calls getEntry
    }

    @Override
    IndexNode getInode(byte[] path) {
        // check for an alias to a versioned entry
        byte[] versionedPath = lookup.apply(path);
        return versionedPath == null ? super.getInode(path) : super.getInode(versionedPath);
    }

}
