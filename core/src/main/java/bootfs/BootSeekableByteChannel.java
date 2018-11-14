package bootfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * executable jars and wars are shell scripts with the jar appended at the end.
 * This seems to work with java, but does not play well with a plugable
 * filesystem. We scan the archive for the Zip magic header set an offset.
 * All operations then take this offset into account.
 * 
 * 
 * @author alpapad
 */
public class BootSeekableByteChannel implements SeekableByteChannel {
    private final SeekableByteChannel channel;
    
    private final long offset;
    /*
     * TODO: Just works, completely unoptimized
     * TODO: Check for the 4 byte magic header + variations
     */
    private final static byte P = (byte) 'P';
    private final static byte K = (byte) 'K';
    static long scanHeader(SeekableByteChannel ch) throws IOException {
        if (ch.size() < 2) {
            return 0;
        }

        final ByteBuffer dst = ByteBuffer.allocate(2);

        for (long pos = 0; pos < ch.size(); pos++) {
            ch.position(pos);
            if (ch.read(dst) == 2) {
                dst.flip();
                if (dst.get(0) == P && dst.get(1) == K) {
                    return pos;
                }
            } else {
                return 0;
            }
        }
        return 0;
    }
    
    public BootSeekableByteChannel(SeekableByteChannel channel) throws IOException {
        super();
        this.channel = channel;
        this.offset = scanHeader(channel);
        this.channel.position(0);
    }
    
    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }
    
    @Override
    public void close() throws IOException {
        channel.close();
    }
    
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }
    
    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
    }
    
    @Override
    public long position() throws IOException {
        return channel.position() - offset;
    }
    
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return channel.position(newPosition + offset);
    }
    
    @Override
    public long size() throws IOException {
        return channel.size() - offset;
    }
    
    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return channel.truncate(size + offset);
    }
    
}
