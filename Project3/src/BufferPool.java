import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Buffer Pool Class
 * 
 * @author kinjalpandey, architg03
 * @version 03/22/2024
 */
public class BufferPool {
    private static final int BLOCK_SIZE = 4096;
    private final LinkedHashMap<Integer, byte[]> cache;
    private final RandomAccessFile diskFile;
    private final int cacheSize;
    private static final int RECORD_SIZE = 4;
    private static final int RECORDS_PER_BLOCK = BLOCK_SIZE / RECORD_SIZE;
    private final Map<Integer, Boolean> dirtyBlocks = new HashMap<>();
    private int cacheHits = 0;
    private int diskReads = 0;
    private int diskWrites = 0;

    /**
     * Constructs a BufferPool instance for managing disk I/O operations with an
     * LRU cache mechanism.
     * 
     * @param filePath
     *            The path of the file to be managed by this buffer pool.
     * @param cacheSize
     *            The number of blocks to keep in the cache.
     * @throws IOException
     *             If there is an issue accessing the file.
     */
    @SuppressWarnings("serial")
    public BufferPool(String filePath, int cacheSize) throws IOException {
        this.cacheSize = cacheSize;
        this.diskFile = new RandomAccessFile(filePath, "rw");

        this.cache = new LinkedHashMap<Integer, byte[]>(cacheSize, 0.75f,
            true) {
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<Integer, byte[]> eldest) {
                return size() > BufferPool.this.cacheSize;
            }
        };
    }


    /**
     * Retrieves the total number of cache hits occurred during the operation of
     * the buffer pool.
     * 
     * @return The total number of cache hits.
     */
    public int getCacheHits() {
        return cacheHits;
    }


    /**
     * Retrieves the total number of disk reads occurred during the operation of
     * the buffer pool.
     * 
     * @return The total number of disk reads.
     */
    public int getDiskReads() {
        return diskReads;
    }


    /**
     * Retrieves the total number of disk writes occurred during the operation
     * of the buffer pool.
     * 
     * @return The total number of disk writes.
     */
    public int getDiskWrites() {
        return diskWrites;
    }


    /**
     * Retrieves a block from the cache or disk, incrementing the appropriate
     * statistics.
     * 
     * @param blockNumber
     *            The block number to retrieve.
     * @return The requested block as a byte array.
     * @throws IOException
     *             If there is an issue reading the block from disk.
     */
    public byte[] getBlock(int blockNumber) throws IOException {
        // Check if the block is already in the cache
        byte[] block = cache.get(blockNumber);
        if (block != null) {
            cacheHits++;
            return block;
        }

        // Block is not in cache; read from disk
        block = new byte[BLOCK_SIZE];
        diskFile.seek((long)blockNumber * BLOCK_SIZE);
        diskFile.readFully(block);
        diskReads++;
        // Add the block to the cache
        cache.put(blockNumber, block);
        return block;
    }


    /**
     * Writes a block back to the disk and updates the cache, marking the block
     * as dirty if modified.
     * 
     * @param blockNumber
     *            The block number to write.
     * @param block
     *            The block data to write.
     * @throws IOException
     *             If there is an issue writing the block to disk.
     */
    public void writeBlock(int blockNumber, byte[] block) throws IOException {
        if (block.length != BLOCK_SIZE) {
            throw new IllegalArgumentException("Block size mismatch.");
        }

        // Write block to disk
        diskWrites++;
        diskFile.seek((long)blockNumber * BLOCK_SIZE);
        diskFile.write(block);
        dirtyBlocks.put(blockNumber, true);

        // Update the cache
        cache.put(blockNumber, block);
    }


    /**
     * Retrieves a record from the file, abstracting the calculation of block
     * number and offset within the block.
     * 
     * @param recordId
     *            The ID of the record to retrieve.
     * @return An array containing the key and value of the record.
     * @throws IOException
     *             If there is an issue accessing the record.
     */
    public short[] getRecord(int recordId) throws IOException {
        int blockNumber = recordId / RECORDS_PER_BLOCK;
        int offset = (recordId % RECORDS_PER_BLOCK) * RECORD_SIZE;
        byte[] block = getBlock(blockNumber);

        short key = (short)(((block[offset] & 0xFF) << 8) | (block[offset + 1]
            & 0xFF));
        short value = (short)(((block[offset + 2] & 0xFF) << 8) | (block[offset
            + 3] & 0xFF));

        return new short[] { key, value };
    }


    /**
     * Writes a record to the file at the specified record ID, updating the
     * appropriate block in the cache or disk.
     * 
     * @param recordId
     *            The ID of the record to write.
     * @param record
     *            The key and value of the record to write.
     * @throws IOException
     *             If there is an issue writing the record.
     */
    public void writeRecord(int recordId, short[] record) throws IOException {
        if (record.length != 2) {
            throw new IllegalArgumentException(
                "Record must have exactly two short values.");
        }
        int blockNumber = recordId / RECORDS_PER_BLOCK;
        int offset = (recordId % RECORDS_PER_BLOCK) * RECORD_SIZE;
        byte[] block = getBlock(blockNumber);

        block[offset] = (byte)(record[0] >> 8);
        block[offset + 1] = (byte)(record[0]);
        block[offset + 2] = (byte)(record[1] >> 8);
        block[offset + 3] = (byte)(record[1]);

        // Mark the block as modified to ensure it's written back to disk later
        // For this, you may need an additional structure to track modified
        // blocks or a custom block object
        writeBlock(blockNumber, block);
    }


    /**
     * Closes the RandomAccessFile associated with this buffer pool, ensuring
     * all resources are released.
     * 
     * @throws IOException
     *             If there is an issue closing the file.
     */
    public void close() throws IOException {
        diskFile.close();
    }


    /**
     * Flushes all dirty blocks from the cache to disk, ensuring data
     * consistency.
     * 
     * @throws IOException
     *             If there is an issue flushing blocks to disk.
     */
    public void flush() throws IOException {
        for (Map.Entry<Integer, Boolean> entry : dirtyBlocks.entrySet()) {
            if (entry.getValue()) { // If the block is marked as dirty
                byte[] block = cache.get(entry.getKey());
                if (block != null) { // If the block is in the cache
                    diskFile.seek(entry.getKey() * BLOCK_SIZE);
                    diskFile.write(block);
                }
            }
        }
        dirtyBlocks.clear(); // Clear the dirty marks after flushing
    }
}
