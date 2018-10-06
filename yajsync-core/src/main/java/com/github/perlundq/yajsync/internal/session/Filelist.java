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
package com.github.perlundq.yajsync.internal.session;

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

import com.github.perlundq.yajsync.attr.FileInfo;

public class Filelist {
    public static class Segment implements Comparable<Integer> {
        private final FileInfo _directory;
        private final int _dirIndex;
        private final int _endIndex;
        private final Map<Integer, FileInfo> _files;
        private long _totalFileSize;
        
        private Segment(FileInfo directory, int dirIndex, List<FileInfo> files, Map<Integer, FileInfo> map, boolean isPruneDuplicates) {
            assert dirIndex >= -1;
            assert files != null;
            assert map != null;
            this._directory = directory; // NOTE: might be null
            this._dirIndex = dirIndex;
            this._endIndex = dirIndex + files.size();
            this._files = map;
            
            int index = dirIndex + 1;
            Collections.sort(files);
            FileInfo prev = null;
            
            for (FileInfo f : files) {
                // Note: we may not remove any other files here (if
                // Receiver) without also notifying Sender with a
                // Filelist.DONE if the Segment ends up being empty
                if (isPruneDuplicates && f.equals(prev)) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning("skipping duplicate " + f);
                    }
                } else {
                    this._files.put(index, f);
                    if (f.attrs().isRegularFile() || f.attrs().isSymbolicLink()) {
                        this._totalFileSize += f.attrs().size();
                    }
                }
                index++;
                prev = f;
            }
        }
        
        // Collections.binarySearch
        @Override
        public int compareTo(Integer other) {
            return this.directoryIndex() - other;
        }
        
        // use bitmap
        private boolean contains(int index) {
            return this._files.containsKey(index);
        }
        
        // generator
        public FileInfo directory() {
            return this._directory;
        }
        
        // generator
        public int directoryIndex() {
            return this._dirIndex;
        }
        
        // generator
        // can be automatically generated or possible removed
        public Iterable<Entry<Integer, FileInfo>> entrySet() {
            return this._files.entrySet();
        }
        
        // generator sender
        public Collection<FileInfo> files() {
            return this._files.values();
        }
        
        // generator sender receiver
        public FileInfo getFileWithIndexOrNull(int index) {
            assert index >= 0;
            return this._files.get(index);
        }
        
        // sender generator
        // use bitmap
        public boolean isFinished() {
            return this._files.isEmpty();
        }
        
        // sender generator
        // use bitmap
        public FileInfo remove(int index) {
            FileInfo f = this._files.remove(index);
            if (f == null) {
                throw new IllegalStateException(String.format("%s does not contain key %d (%s)", this._files, index, this));
            }
            return f;
        }
        
        // generator
        // use bitmap
        public void removeAll() {
            this._files.clear();
        }
        
        // generator
        // inefficient, can possibly be removed
        public void removeAll(Collection<Integer> toRemove) {
            for (int i : toRemove) {
                this._files.remove(i);
            }
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            int active = this._files.values().size();
            int size = this._endIndex - this._dirIndex;
            sb.append(String.format("%s [%s, dirIndex=%d, fileIndices=%d:%d, size=%d/%d]", this.getClass().getSimpleName(), this._directory != null ? this._directory : "-", this._dirIndex,
                    this._dirIndex + 1, this._endIndex, active, size));
            
            if (_log.isLoggable(Level.FINEST)) {
                for (Map.Entry<Integer, FileInfo> e : this._files.entrySet()) {
                    sb.append("   ").append(e.getValue()).append(", ").append(e.getKey());
                }
            }
            
            return sb.toString();
        }
    }
    
    public static class SegmentBuilder {
        private List<FileInfo> _directories = new ArrayList<>();
        private FileInfo _directory;
        private List<FileInfo> _files = new ArrayList<>();
        
        public SegmentBuilder(FileInfo directory) {
            this._directory = directory;
        }
        
        public SegmentBuilder(FileInfo directory, List<FileInfo> files, List<FileInfo> directories) {
            this._directory = directory;
            this._files = files;
            this._directories = directories;
        }
        
        /**
         * @throws IllegalStateException if the path of fileInfo is not below segment
         *                               directory path
         */
        public void add(FileInfo fileInfo) {
            assert this._files != null && this._directories != null;
            assert fileInfo != null;
            this._files.add(fileInfo);
            // NOTE: we store the directory in the builder regardless if we're
            // using recursive transfer or not
            // NOTE: we must also store DOT_DIR since this is what a native
            // sender does
            if (fileInfo.attrs().isDirectory()) {
                this._directories.add(fileInfo);
            }
        }
        
        public void addAll(Iterable<FileInfo> fileset) {
            for (FileInfo f : fileset) {
                this.add(f);
            }
        }
        
        private void clear() {
            this._directory = null;
            this._files = null;
            this._directories = null;
        }
        
        public List<FileInfo> directories() {
            return this._directories;
        }
        
        public FileInfo directory() {
            return this._directory;
        }
        
        public List<FileInfo> files() {
            return this._files;
        }
        
        @Override
        public String toString() {
            return String.format("%s (directory=%s, stubDirectories=%s, " + "files=%s)%n", this.getClass().getSimpleName(), this._directory, this._directories, this._files);
        }
    }
    
    public static final Logger _log = Logger.getLogger(Filelist.class.getName());
    public static final int DONE = -1; // done with segment, may be deleted
    public static final int EOF = -2; // no more segments in file list
    public static final int OFFSET = -101;
    
    private final boolean _isPruneDuplicates;
    private final boolean _isRecursive;
    private int _nextDirIndex;
    private int _numFiles;
    protected final List<Segment> _segments;
    private final SortedMap<Integer, FileInfo> _stubDirectories;
    private int _stubDirectoryIndex = 0;
    private long _totalFileSize;
    
    public Filelist(boolean isRecursive, boolean isPruneDuplicates) {
        this(isRecursive, isPruneDuplicates, new ArrayList<Segment>());
    }
    
    protected Filelist(boolean isRecursive, boolean isPruneDuplicates, List<Segment> segments) {
        this._segments = segments;
        this._isRecursive = isRecursive;
        this._isPruneDuplicates = isPruneDuplicates;
        if (isRecursive) {
            this._stubDirectories = new TreeMap<>();
            this._nextDirIndex = 0;
        } else {
            this._stubDirectories = null;
            this._nextDirIndex = -1;
        }
    }
    
    // sender receiver generator
    public Segment deleteFirstSegment() {
        return this._segments.remove(0);
    }
    
    public int expandedSegments() {
        return this._segments.size();
    }
    
    private void extractStubDirectories(List<FileInfo> directories) {
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("extracting all stub directories from " + directories);
        }
        
        Collections.sort(directories);
        for (FileInfo f : directories) {
            assert f.attrs().isDirectory();
            if (!((FileInfoImpl) f).isDotDir()) {
                if (_log.isLoggable(Level.FINER)) {
                    _log.finer(String.format("adding non dot dir %s with index=%d to stub " + "directories for later expansion", f, this._stubDirectoryIndex));
                }
                this._stubDirectories.put(this._stubDirectoryIndex, f);
            }
            this._stubDirectoryIndex++;
        }
    }
    
    public Segment firstSegment() {
        return this._segments.get(0);
    }
    
    // NOTE: fileIndex may be directoryIndex too
    // sender receiver generator
    public Segment getSegmentWith(int index) {
        assert index >= 0;
        
        int result = Collections.binarySearch(this._segments, index);
        if (result >= 0) {
            return this._segments.get(result);
        }
        int insertionPoint = -result - 1;
        int segmentIndex = insertionPoint - 1;
        if (segmentIndex < 0) {
            return null;
        }
        Segment segment = this._segments.get(segmentIndex);
        return segment.contains(index) ? segment : null;
    }
    
    // sender receiver
    /**
     * @throws RuntimeException if directoryIndex not in range
     */
    public FileInfo getStubDirectoryOrNull(int directoryIndex) {
        if (directoryIndex < this._stubDirectories.firstKey() || directoryIndex > this._stubDirectories.lastKey()) {
            throw new RuntimeException(String.format("%d not within [%d:%d] (%s)", directoryIndex, this._stubDirectories.firstKey(), this._stubDirectories.lastKey(), this));
        }
        return this._stubDirectories.remove(directoryIndex);
    }
    
    // sender receiver
    public boolean isEmpty() {
        return this._segments.isEmpty() && !this.isExpandable();
    }
    
    // sender receiver
    public boolean isExpandable() {
        return this._stubDirectories != null && this._stubDirectories.size() > 0;
    }
    
    public Segment newSegment(SegmentBuilder builder) {
        return this.newSegment(builder, new TreeMap<Integer, FileInfo>());
    }
    
    protected Segment newSegment(SegmentBuilder builder, SortedMap<Integer, FileInfo> map) {
        assert builder._directory == null == (this._isRecursive && this._nextDirIndex == 0 || !this._isRecursive && this._nextDirIndex == -1);
        assert builder._directories != null;
        assert builder._files != null;
        
        if (_log.isLoggable(Level.FINER)) {
            _log.finer(String.format("creating new segment from builder=%s and map=%s", builder, map));
        }
        
        if (this._isRecursive) {
            this.extractStubDirectories(builder._directories);
        }
        Segment segment = new Segment(builder._directory, this._nextDirIndex, builder._files, map, this._isPruneDuplicates);
        builder.clear();
        this._nextDirIndex = segment._endIndex + 1;
        this._segments.add(segment);
        this._totalFileSize += segment._totalFileSize;
        this._numFiles += segment._files.size();
        return segment;
    }
    
    public int numFiles() {
        return this._numFiles;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s isExpandable=%s (", this.getClass().getSimpleName(), this.isExpandable()));
        
        for (Segment s : this._segments) {
            String str = s._directory == null || s._directory.pathName() == null ? "-" : s._directory.toString();
            sb.append(", ").append(String.format("segment(%d, %s)", s.directoryIndex(), str));
        }
        sb.append(")\n").append("unexpanded: ").append(this._stubDirectories);
        return sb.toString();
    }
    
    public long totalFileSize() {
        return this._totalFileSize;
    }
}
