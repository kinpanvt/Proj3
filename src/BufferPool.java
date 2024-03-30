import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
        this.cache = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<Integer, byte[]> eldest) {
                boolean shouldRemove = size() > BufferPool.this.cacheSize;
                if (shouldRemove) {
                    try {
                        if (dirtyBlocks.getOrDefault(eldest.getKey(), false)) {
                            diskWrites++;
                            diskFile.seek(eldest.getKey() * (long)BLOCK_SIZE);
                            diskFile.write(eldest.getValue());
                            dirtyBlocks.remove(eldest.getKey());
                        }
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return shouldRemove;
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
        byte[] block = cache.get(blockNumber);
        if (block != null) {
            cacheHits++;
            return block;
        }
        block = new byte[BLOCK_SIZE];
        diskFile.seek(blockNumber * (long)BLOCK_SIZE);
        diskFile.readFully(block);
        diskReads++;
        cache.put(blockNumber, block);
        return block;
    }


    /**
     * Mark the block as dirty without writing it to disk immediately.
     * 
     * @param blockNumber
     *            The block number to retrieve.
     * @param blockData
     *            Data in block
     */
    public void markBlockAsDirty(int blockNumber, byte[] blockData) {
        // Only updating the cache and dirtyBlocks if necessary.
        if (!dirtyBlocks.containsKey(blockNumber)) {
            cache.put(blockNumber, blockData);
            dirtyBlocks.put(blockNumber, true);
        }
        // no need to re-mark it or re-put it in the cache.
    }


    /**
     * Mark the block as dirty without writing it to disk immediately.
     * 
     * @param blockNumber
     *            Block to write
     */
    public void markBlockAsDirty(int blockNumber) {
        dirtyBlocks.put(blockNumber, true);
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
        cache.put(blockNumber, block);
        markBlockAsDirty(blockNumber);
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

        // Instead of writing to disk, mark the block as dirty.
        markBlockAsDirty(blockNumber, block);
    }


    /**
     * Closes the RandomAccessFile associated with this buffer pool, ensuring
     * all resources are released.
     * 
     * @throws IOException
     *             If there is an issue closing the file.
     */
    public void close() throws IOException {
        flush();
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
            if (entry.getValue()) {
                byte[] block = cache.get(entry.getKey());
                if (block != null) {
                    diskFile.seek(entry.getKey() * (long)BLOCK_SIZE);
                    diskFile.write(block);
                    diskWrites++;
                }
            }
        }
        dirtyBlocks.clear();
    }


    /**
     * Different ways of flushing blocks for binary blocks
     * 
     * @throws IOException
     *             exceptions
     */
    public void flushDirtyBlocks() throws IOException {
        // Sort keys to minimize disk head movement
        List<Integer> keys = new ArrayList<>(dirtyBlocks.keySet());
        Collections.sort(keys);
        for (Integer key : keys) {
            if (dirtyBlocks.get(key)) {
                byte[] block = cache.get(key);
                diskFile.seek(key * (long)BLOCK_SIZE);
                diskFile.write(block);
                diskWrites++;
                dirtyBlocks.put(key, false); // Mark as clean
            }
        }
    }
}
