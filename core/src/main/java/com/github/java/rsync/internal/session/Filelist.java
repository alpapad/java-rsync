/*
 * Rsync file information list
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.java.rsync.internal.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.attr.FileInfo;

public class Filelist {
    public static class Segment implements Comparable<Integer> {
        private final FileInfo directory;
        private final int dirIndex;
        private final int endIndex;
        private final Map<Integer, FileInfo> files;
        private long totalFileSize;

        private Segment(FileInfo directory, int dirIndex, List<FileInfo> files, Map<Integer, FileInfo> map, boolean isPruneDuplicates) {
            assert dirIndex >= -1;
            assert files != null;
            assert map != null;
            this.directory = directory; // NOTE: might be null
            this.dirIndex = dirIndex;
            endIndex = dirIndex + files.size();
            this.files = map;

            int index = dirIndex + 1;
            Collections.sort(files);
            FileInfo prev = null;

            for (FileInfo f : files) {
                // Note: we may not remove any other files here (if
                // Receiver) without also notifying Sender with a
                // Filelist.DONE if the Segment ends up being empty
                if (isPruneDuplicates && f.equals(prev)) {
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning("skipping duplicate " + f);
                    }
                } else {
                    this.files.put(index, f);
                    if (f.getAttributes().isRegularFile() || f.getAttributes().isSymbolicLink()) {
                        totalFileSize += f.getAttributes().getSize();
                    }
                }
                index++;
                prev = f;
            }
        }

        // Collections.binarySearch
        @Override
        public int compareTo(Integer other) {
            return getDirectoryIndex() - other;
        }

        // use bitmap
        private boolean contains(int index) {
            return files.containsKey(index);
        }

        // generator
        // can be automatically generated or possible removed
        public Iterable<Entry<Integer, FileInfo>> entrySet() {
            return files.entrySet();
        }

        // generator
        public FileInfo getDirectory() {
            return directory;
        }

        // generator
        public int getDirectoryIndex() {
            return dirIndex;
        }

        // generator sender
        public Collection<FileInfo> getFiles() {
            return files.values();
        }

        // generator sender receiver
        public FileInfo getFileWithIndexOrNull(int index) {
            assert index >= 0;
            return files.get(index);
        }

        // sender generator
        // use bitmap
        public boolean isFinished() {
            return files.isEmpty();
        }

        // sender generator
        // use bitmap
        public FileInfo remove(int index) {
            FileInfo f = files.remove(index);
            if (f == null) {
                throw new IllegalStateException(String.format("%s does not contain key %d (%s)", files, index, this));
            }
            return f;
        }

        // generator
        // use bitmap
        public void removeAll() {
            files.clear();
        }

        // generator
        // inefficient, can possibly be removed
        public void removeAll(Collection<Integer> toRemove) {
            for (int i : toRemove) {
                files.remove(i);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            int active = files.values().size();
            int size = endIndex - dirIndex;
            sb.append(String.format("%s [%s, dirIndex=%d, fileIndices=%d:%d, size=%d/%d]", this.getClass().getSimpleName(), directory != null ? directory : "-", dirIndex, dirIndex + 1, endIndex,
                    active, size));

            if (LOG.isLoggable(Level.FINEST)) {
                for (Map.Entry<Integer, FileInfo> e : files.entrySet()) {
                    sb.append("   ").append(e.getValue()).append(", ").append(e.getKey());
                }
            }

            return sb.toString();
        }
    }

    public static class SegmentBuilder {
        private List<FileInfo> directories = new ArrayList<>();
        private FileInfo directory;
        private List<FileInfo> files = new ArrayList<>();

        public SegmentBuilder(FileInfo directory) {
            this.directory = directory;
        }

        public SegmentBuilder(FileInfo directory, List<FileInfo> files, List<FileInfo> directories) {
            this.directory = directory;
            this.files = files;
            this.directories = directories;
        }

        /**
         * @throws IllegalStateException if the path of fileInfo is not below segment
         *                               directory path
         */
        public void add(FileInfo fileInfo) {
            assert files != null && directories != null;
            assert fileInfo != null;
            files.add(fileInfo);
            // NOTE: we store the directory in the builder regardless if we're
            // using recursive transfer or not
            // NOTE: we must also store DOT_DIR since this is what a native
            // sender does
            if (fileInfo.getAttributes().isDirectory()) {
                directories.add(fileInfo);
            }
        }

        public void addAll(Iterable<FileInfo> fileset) {
            for (FileInfo f : fileset) {
                add(f);
            }
        }

        private void clear() {
            directory = null;
            files = null;
            directories = null;
        }

        public List<FileInfo> getDirectories() {
            return directories;
        }

        public FileInfo getDirectory() {
            return directory;
        }

        public List<FileInfo> getFiles() {
            return files;
        }

        @Override
        public String toString() {
            return String.format("%s (directory=%s, stubDirectories=%s, " + "files=%s)%n", this.getClass().getSimpleName(), directory, directories, files);
        }
    }

    public static final int DONE = -1; // done with segment, may be deleted
    public static final int EOF = -2; // no more segments in file list
    public static final Logger LOG = Logger.getLogger(Filelist.class.getName());
    public static final int OFFSET = -101;

    private int nextDirIndex;
    private int numFiles;
    private final boolean pruneDuplicates;
    private final boolean recursive;
    protected final List<Segment> segments;
    private final SortedMap<Integer, FileInfo> stubDirectories;
    private int stubDirectoryIndex = 0;
    private long totalFileSize;

    public Filelist(boolean isRecursive, boolean isPruneDuplicates) {
        this(isRecursive, isPruneDuplicates, new ArrayList<Segment>());
    }

    protected Filelist(boolean isRecursive, boolean isPruneDuplicates, List<Segment> segments) {
        this.segments = segments;
        recursive = isRecursive;
        pruneDuplicates = isPruneDuplicates;
        if (isRecursive) {
            stubDirectories = new TreeMap<>();
            nextDirIndex = 0;
        } else {
            stubDirectories = null;
            nextDirIndex = -1;
        }
    }

    // sender receiver generator
    public Segment deleteFirstSegment() {
        return segments.remove(0);
    }

    private void extractStubDirectories(List<FileInfo> directories) {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("extracting all stub directories from " + directories);
        }

        Collections.sort(directories);
        for (FileInfo f : directories) {
            assert f.getAttributes().isDirectory();
            if (!((FileInfoImpl) f).isDotDir()) {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(String.format("adding non dot dir %s with index=%d to stub " + "directories for later expansion", f, stubDirectoryIndex));
                }
                stubDirectories.put(stubDirectoryIndex, f);
            }
            stubDirectoryIndex++;
        }
    }

    public int getExpandedSegments() {
        return segments.size();
    }

    public Segment getFirstSegment() {
        return segments.get(0);
    }

    public int getNumFiles() {
        return numFiles;
    }

    // NOTE: fileIndex may be directoryIndex too
    // sender receiver generator
    public Segment getSegmentWith(int index) {
        assert index >= 0;

        int result = Collections.binarySearch(segments, index);
        if (result >= 0) {
            return segments.get(result);
        }
        int insertionPoint = -result - 1;
        int segmentIndex = insertionPoint - 1;
        if (segmentIndex < 0) {
            return null;
        }
        Segment segment = segments.get(segmentIndex);
        return segment.contains(index) ? segment : null;
    }

    // sender receiver
    /**
     * @throws RuntimeException if directoryIndex not in range
     */
    public FileInfo getStubDirectoryOrNull(int directoryIndex) {
        if (directoryIndex < stubDirectories.firstKey() || directoryIndex > stubDirectories.lastKey()) {
            throw new RuntimeException(String.format("%d not within [%d:%d] (%s)", directoryIndex, stubDirectories.firstKey(), stubDirectories.lastKey(), this));
        }
        return stubDirectories.remove(directoryIndex);
    }

    public long getTotalFileSize() {
        return totalFileSize;
    }

    // sender receiver
    public boolean isEmpty() {
        return segments.isEmpty() && !isExpandable();
    }

    // sender receiver
    public boolean isExpandable() {
        return stubDirectories != null && stubDirectories.size() > 0;
    }

    public Segment newSegment(SegmentBuilder builder) {
        return this.newSegment(builder, new TreeMap<Integer, FileInfo>());
    }

    protected Segment newSegment(SegmentBuilder builder, SortedMap<Integer, FileInfo> map) {
        assert builder.directory == null == (recursive && nextDirIndex == 0 || !recursive && nextDirIndex == -1);
        assert builder.directories != null;
        assert builder.files != null;

        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer(String.format("creating new segment from builder=%s and map=%s", builder, map));
        }

        if (recursive) {
            extractStubDirectories(builder.directories);
        }
        Segment segment = new Segment(builder.directory, nextDirIndex, builder.files, map, pruneDuplicates);
        builder.clear();
        nextDirIndex = segment.endIndex + 1;
        segments.add(segment);
        totalFileSize += segment.totalFileSize;
        numFiles += segment.files.size();
        return segment;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s isExpandable=%s (", this.getClass().getSimpleName(), isExpandable()));

        for (Segment s : segments) {
            String str = s.directory == null || s.directory.getPathName() == null ? "-" : s.directory.toString();
            sb.append(", ").append(String.format("segment(%d, %s)", s.getDirectoryIndex(), str));
        }
        sb.append(")\n").append("unexpanded: ").append(stubDirectories);
        return sb.toString();
    }
}
