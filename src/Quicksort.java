
/**
 * Quicksort is a adaptation of the Quicksort algorithm, designed to sort
 * large datasets that do not fit entirely into main memory.
 * It leverages a disk-backed storage mechanism, through the
 * use of a buffer pool, to temporarily store and access data that exceeds the
 * capacity of the main memory. This approach modifies the standard Quicksort by
 * integrating it with a virtual memory system, where the file on the disk acts
 * as an extended array, and all operations on this virtual array are mediated
 * by the buffer pool.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The class containing the main method.
 *
 * @author kinjalpandey, architg03
 * @version 03/22/2024
 */

// On my honor:
//
// - I have not used source code obtained from another student,
// or any other unauthorized source, either modified or
// unmodified.
//
// - All source code and documentation used in my program is
// either my original work, or was derived by me from the
// source code published in the textbook for this course.
//
// - I have not discussed coding details about this project with
// anyone other than my partner (in the case of a joint
// submission), instructor, ACM/UPE tutors or the TAs assigned
// to this course. I understand that I may discuss the concepts
// of this program with other students, and that another student
// may help me debug my program so long as neither of us writes
// anything during the discussion or modifies any computer file
// during the discussion. I have violated neither the spirit nor
// letter of this restriction.

public class Quicksort {

    private BufferPool bufferPool;
    private static final int RECORD_SIZE = 4;
    private static final int BLOCK_SIZE = 4096;
    @SuppressWarnings("unused")
    private static final int RECORDS_PER_BLOCK = BLOCK_SIZE / RECORD_SIZE;
    private static Quicksort qs;
    private static final int THRESHOLD = 1500;

    /**
     * Default Constructor
     * 
     * @param filePath
     *            path of file
     * @param cacheSize
     *            size of cache
     * @throws IOException
     *             error handling
     */
    public Quicksort(String filePath, int cacheSize) throws IOException {
        this.bufferPool = new BufferPool(filePath, cacheSize);

    }


    /**
     * main() for the function.
     * 
     * @param args
     *            Command line parameters.
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Quicksort <data-file-name> "
                + "<numb-buffers> <stat-file-name>");
            return;
        }
        String filename = args[0];
        int numbBuffers = Integer.parseInt(args[1]);
        String statFileName = args[2];
        try {
            long startTime = System.currentTimeMillis();
            sortFile(filename, numbBuffers); // Pass numbBuffers to sortFile
            long endTime = System.currentTimeMillis();

            try {
                qs.writeStatistics(filename, numbBuffers, statFileName, endTime
                    - startTime);

            }
            catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("File has been sorted.");
        }
        catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * Initiates the sorting of the specified file using a disk-backed Quicksort
     * algorithm.
     * This method sets up the buffer pool with the specified number of buffers
     * and performs
     * the sorting operation on the data file represented on the disk. The
     * sorted data
     * is written back to the same file, effectively modifying its contents to
     * be in sorted order.
     *
     * @param filename
     *            The path to the data file that needs to be sorted.
     * @param numbBuffers
     *            The number of buffers to be allocated for the buffer pool,
     *            affecting the efficiency and performance of the sorting
     *            operation.
     * @throws Exception
     *             If any error occurs during file processing, including I/O
     *             errors
     *             or issues initializing the buffer pool.
     */
    public static void sortFile(String filename, int numbBuffers)
        throws Exception {
        qs = new Quicksort(filename, numbBuffers);
        long fileSize = new File(filename).length();
        int totalRecords = (int)(fileSize / RECORD_SIZE);
        qs.quickSort(0, totalRecords - 1);
        qs.finalizeSorting();
    }


    /**
     * Performs the Quicksort algorithm on a virtual array represented by the
     * data file on disk.
     * This method recursively sorts the segment of the array between the
     * specified indices
     * using the Quicksort algorithm, with modifications to operate over a
     * disk-backed storage
     * system through the buffer pool.
     *
     * @param low
     *            The starting index of the segment of the array to be sorted.
     * @param high
     *            The ending index of the segment of the array to be sorted.
     * @throws Exception
     *             If any error occurs during the sorting process, including
     *             issues
     *             accessing or modifying the data through the buffer pool.
     */
    public void quickSort(int low, int high) throws Exception {
        if (low < high) {
            if (high - low > THRESHOLD) {
                // Use three-way partitioning for larger segments
                int[] partitionIndices = threeWayPartition(low, high);
                quickSort(low, partitionIndices[0] - 1); // Sort elements less
                                                         // than pivot
                quickSort(partitionIndices[1] + 1, high); // Sort elements
                                                          // greater than pivot
            }
            else {
                // Use traditional partitioning for smaller segments
                int pi = partition(low, high);
                quickSort(low, pi - 1);
                quickSort(pi + 1, high);
            }
        }
    }


    private int medianOfThreePivotIndex(int low, int high) throws Exception {
        int mid = low + (high - low) / 2;
        short[] start = bufferPool.getRecord(low);
        short[] middle = bufferPool.getRecord(mid);
        short[] end = bufferPool.getRecord(high);

        // Ordering the start, middle, end to find the median
        if (start[0] > end[0]) {
            short[] temp = start;
            start = end;
            end = temp;
        }
        if (middle[0] > end[0]) {
            mid = high;
        }
        else if (middle[0] > start[0]) {
            mid = low;
        }

        return mid;
    }


    private int partition(int low, int high) throws Exception {
        int pivotIndex = medianOfThreePivotIndex(low, high);
        // Swap pivot with high to use existing partition logic
        swapRecords(pivotIndex, high);
        short[] pivot = bufferPool.getRecord(high);

        int i = low - 1;
        for (int j = low; j < high; j++) {
            short[] record = bufferPool.getRecord(j);
            if (record[0] < pivot[0]) {
                i++;
                swapRecords(i, j);
            }
        }
        swapRecords(i + 1, high);
        return i + 1;
    }


    private void swapRecords(int index1, int index2) throws Exception {
        short[] record1 = bufferPool.getRecord(index1);
        short[] record2 = bufferPool.getRecord(index2);

        bufferPool.writeRecord(index1, record2);
        bufferPool.writeRecord(index2, record1);
    }


    private int medianOfThreePivotIndex(int low, int mid, int high)
        throws Exception {
        short[] start = bufferPool.getRecord(low);
        short[] middle = bufferPool.getRecord(mid);
        short[] end = bufferPool.getRecord(high);

        // Ordering the start, middle, end to find the median
        if (start[0] > end[0]) {
            short[] temp = start;
            start = end;
            end = temp;
        }
        if (middle[0] > end[0]) {
            mid = high;
        }
        else if (middle[0] > start[0]) {
            mid = low;
        }

        return mid;
    }


    private int[] threeWayPartition(int low, int high) throws Exception {
        int pivotIndex = medianOfThreePivotIndex(low, (low + high) / 2, high);
        swapRecords(low, pivotIndex); // Move pivot to start for simplicity

        short[] pivot = bufferPool.getRecord(low);
        int lt = low;
        int gt = high;
        int i = low + 1;
        while (i <= gt) {
            short[] current = bufferPool.getRecord(i);
            if (current[0] < pivot[0]) {
                swapRecords(lt++, i++);
            }
            else if (current[0] > pivot[0]) {
                swapRecords(i, gt--);
            }
            else {
                i++;
            }
        }
        return new int[] { lt, gt }; // Return indices bounding elements equal
                                     // to pivot
    }


    /**
     * Writes the statistics of the sorting operation to a specified file. The
     * statistics
     * include the number of cache hits, disk reads, and disk writes encountered
     * during
     * the sorting process, as well as the total time taken to complete the
     * sort.
     * This information is appended to the specified statistics file, allowing
     * for
     * multiple runs to be documented consecutively.
     *
     * @param filename
     *            The name of the data file that was sorted.
     * @param numbBuffers
     *            The number of buffers used in the buffer pool for the sorting
     *            operation.
     * @param statFileName
     *            The path to the file where the statistics should be written.
     * @param sortTime
     *            The total time taken to sort the data file, in milliseconds.
     * @throws IOException
     *             If an I/O error occurs while writing to the statistics file.
     */
    public void writeStatistics(
        String filename,
        int numbBuffers,
        String statFileName,
        long sortTime)
        throws IOException {
        try (FileWriter fw = new FileWriter(statFileName, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)) {
            out.println("Standard sort on " + filename);
            out.println("Cache Hits: " + this.bufferPool.getCacheHits());
            out.println("Disk Reads: " + this.bufferPool.getDiskReads());
            out.println("Disk Writes: " + this.bufferPool.getDiskWrites());
            out.println("Sort Time: " + sortTime + "ms");
        }
    }


    /**
     * Finalizes the sorting operation by ensuring all modified data blocks in
     * the buffer pool
     * are written back to the disk. This method should be called after the
     * sorting algorithm
     * completes to ensure the data file on disk is fully updated and reflects
     * the sorted order
     * of the records. It guarantees the integrity and persistence of the sorted
     * data.
     *
     * @throws Exception
     *             If any error occurs during the flushing process, including
     *             I/O errors
     *             writing the buffered data to disk.
     */
    public void finalizeSorting() throws Exception {
        bufferPool.flush();
    }

}
