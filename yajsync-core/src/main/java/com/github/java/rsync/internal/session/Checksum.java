/*
 * Rsync file checksum information
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014 Per Lundqvist
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.github.java.rsync.internal.util.Multimap;

class Checksum {
    public static class Chunk {
        
        private final int chunkIndex;
        private final int length;
        private final byte[] md5sum;
        
        private Chunk(int length, byte[] md5sum, int chunkIndex) {
            assert length >= 0;
            assert md5sum != null;
            assert md5sum.length > 0;
            this.length = length;
            this.md5sum = md5sum;
            this.chunkIndex = chunkIndex;
        }
        
        public int getChunkIndex() {
            return this.chunkIndex;
        }
        
        public int getLength() {
            return this.length;
        }
        
        public byte[] getMd5Checksum() {
            return this.md5sum;
        }
    }
    
    public static class ChunkOverflow extends Exception {
        private static final long serialVersionUID = 1L;
        
        ChunkOverflow(String message) {
            super(message);
        }
    }
    
    public static class Header {
        private final int blockLength; // sum_struct.blength
        private final int chunkCount;
        private final int digestLength; // sum_struct.s2length
        private final int remainder; // sum_struct.remainder
        
        /**
         * @throws IllegalArgumentException
         */
        public Header(int chunkCount, int blockLength, int remainder, int digestLength) {
            if (chunkCount < 0) {
                throw new IllegalArgumentException("illegal chunk count" + chunkCount);
            } else if (blockLength == 0 && chunkCount > 0) {
                throw new IllegalArgumentException(
                        String.format("illegal invariant: block length == %d and chunk count " + "== %d - expected chunk count 0 if block length is 0", blockLength, chunkCount));
            } else if (blockLength < 0 || blockLength > MAX_CHECKSUM_BLOCK_LENGTH) {
                throw new IllegalArgumentException(String.format("illegal block length: %d (expected >= 0 && < %d)", blockLength, MAX_CHECKSUM_BLOCK_LENGTH));
            } else if (remainder < 0 || remainder > blockLength) {
                throw new IllegalArgumentException(String.format("Error: invalid remainder length : %d (block length " + "== %d)", remainder, blockLength));
            } else if (digestLength < 0) {
                throw new IllegalArgumentException("Error: invalid checksum " + "digest length: " + digestLength);
            }
            this.blockLength = blockLength;
            this.digestLength = digestLength;
            this.remainder = remainder;
            this.chunkCount = chunkCount;
        }
        
        public Header(int blockLength, int digestLength, long fileSize) throws ChunkOverflow {
            if (blockLength == 0) {
                assert digestLength == 0 : digestLength;
                assert fileSize == 0 : fileSize;
                this.blockLength = 0;
                this.digestLength = 0;
                this.remainder = 0;
                this.chunkCount = 0;
            } else {
                this.blockLength = blockLength;
                this.digestLength = digestLength;
                this.remainder = (int) (fileSize % blockLength);
                long chunkCount = fileSize / blockLength + (this.remainder > 0 ? 1 : 0);
                if (chunkCount >= 0 && chunkCount <= Integer.MAX_VALUE) {
                    this.chunkCount = (int) chunkCount;
                } else {
                    throw new ChunkOverflow(String.format("chunk count is negative or greater " + "than int max: %d > %d", chunkCount, Integer.MAX_VALUE));
                }
            }
        }
        
        public int getBlockLength() {
            return this.blockLength;
        }
        
        public int getChunkCount() {
            return this.chunkCount;
        }
        
        public int getDigestLength() {
            return this.digestLength;
        }
        
        public int getRemainder() {
            return this.remainder;
        }
        
        public int getSmallestChunkSize() {
            if (this.remainder > 0) {
                return this.remainder;
            } else {
                return this.blockLength; // NOTE: might return 0
            }
        }
        
        @Override
        public String toString() {
            return String.format("%s (blockLength=%d, remainder=%d, " + "chunkCount=%d, digestLength=%d)", this.getClass().getSimpleName(), this.blockLength, this.remainder, this.chunkCount,
                    this.digestLength);
        }
    }
    
    private static final Iterable<Chunk> EMPTY_ITERABLE = new Iterable<Chunk>() {
        @Override
        public Iterator<Chunk> iterator() {
            return Collections.emptyIterator();
        }
    };
    private static final int MAX_CHECKSUM_BLOCK_LENGTH = 1 << 17;
    
    public static final int MAX_DIGEST_LENGTH = 16;
    public static final int MIN_DIGEST_LENGTH = 2;
    
    private final Header header;
    private final Multimap<Integer, Chunk> sums;
    
    public Checksum(Header header) {
        this.header = header;
        this.sums = new Multimap<>(header.getChunkCount());
    }
    
    public void addChunkInformation(int rolling, byte[] md5sum) {
        assert md5sum != null;
        assert md5sum.length >= MIN_DIGEST_LENGTH && md5sum.length <= MAX_DIGEST_LENGTH;
        assert this.sums.size() <= this.header.chunkCount - 1;
        
        int chunkIndex = this.sums.size();
        int chunkLength = this.chunkLengthFor(chunkIndex);
        Chunk chunk = new Chunk(chunkLength, md5sum, chunkIndex);
        this.sums.put(rolling, chunk);
    }
    
    private int binarySearch(List<Chunk> chunks, int chunkIndex) {
        int i_left = 0;
        int i_right = chunks.size() - 1;
        
        while (i_left <= i_right) {
            int i_middle = i_left + (i_right - i_left) / 2; // i_middle < i_right and i_middle >= i_left
            int chunkIndex_m = chunks.get(i_middle).getChunkIndex();
            if (chunkIndex_m == chunkIndex) {
                return i_middle;
            } else if (chunkIndex_m < chunkIndex) {
                i_left = i_middle + 1;
            } else { // chunkIndex_m > chunkIndex
                i_right = i_middle - 1;
            }
        }
        /*
         * return i_left as the insertion point - the first index with a chunk index
         * greater than chunkIndex. i_left >= 0 and i_left <= chunks.size()
         */
        return -i_left - 1;
    }
    
    private int chunkLengthFor(int chunkIndex) {
        boolean isLastChunkIndex = chunkIndex == this.header.chunkCount - 1;
        if (isLastChunkIndex && this.header.remainder > 0) {
            return this.header.remainder;
        }
        return this.header.blockLength;
    }
    
    // retrieve a close index for the chunk with the supplied chunk index
    private int closeIndexOf(List<Chunk> chunks, int chunkIndex) {
        int idx = this.binarySearch(chunks, chunkIndex);
        if (idx < 0) {
            int insertionPoint = -idx - 1;
            return Math.min(insertionPoint, chunks.size() - 1);
        }
        return idx;
    }
    
    public Iterable<Chunk> getCandidateChunks(int rolling, final int length, final int preferredChunkIndex) {
        final List<Chunk> chunks = this.sums.get(rolling);
        // Optimization to avoid allocating tons of empty iterators for non
        // matching entries on large files:
        if (chunks.isEmpty()) {
            return EMPTY_ITERABLE;
        }
        
        // return an Iterable which filters out chunks with non matching length
        // and starts with preferredIndex (or close to preferredIndex)
        return new Iterable<Chunk>() {
            int initialIndex = Checksum.this.closeIndexOf(chunks, preferredChunkIndex);
            boolean isInitial = true;
            int it_index = 0;
            
            @Override
            public Iterator<Chunk> iterator() {
                return new Iterator<Chunk>() {
                    
                    @Override
                    public boolean hasNext() {
                        if (isInitial) {
                            return true;
                        }
                        it_index = this.nextIndex();
                        return it_index < chunks.size();
                    }
                    
                    @Override
                    public Chunk next() {
                        try {
                            Chunk c;
                            if (isInitial) {
                                c = chunks.get(initialIndex);
                                isInitial = false;
                            } else {
                                c = chunks.get(it_index);
                                it_index++;
                            }
                            return c;
                        } catch (IndexOutOfBoundsException e) {
                            throw new NoSuchElementException(e.getMessage());
                        }
                    }
                    
                    private int nextIndex() {
                        for (int i = it_index; i < chunks.size(); i++) {
                            if (i != initialIndex && chunks.get(i).getLength() == length) {
                                return i;
                            }
                        }
                        return chunks.size();
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
    
    public Header getHeader() {
        return this.header;
    }
    
    @Override
    public String toString() {
        return String.format("%s (header=%s)", this.getClass().getSimpleName(), this.header);
    }
}
