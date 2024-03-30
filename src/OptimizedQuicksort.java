import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A different approach to quicksort.
 * 
 * @author kinjalpandey, architg03
 * @version 03/22/2024
 */
public class OptimizedQuicksort {

    private BufferPool bufferPool;
    private int totalRecords;

    /**
     * Constructor for OptimizedQuicksort.
     * 
     * @param bufferPool
     *            The buffer pool used for disk I/O operations.
     * @param totalRecords
     *            The total number of records to be sorted.
     */
    public OptimizedQuicksort(BufferPool bufferPool, int totalRecords) {
        this.bufferPool = bufferPool;
        this.totalRecords = totalRecords;
    }


    /**
     * The main method to perform the quicksort algorithm on the entire dataset.
     * It loads the dataset into memory, performs the sorting, and writes it
     * back.
     * 
     * @throws IOException
     *             If there is an issue with buffer pool operations.
     */
    public void quickSort() throws IOException {
        // Load the entire dataset into memory
        List<short[]> data = new ArrayList<>(totalRecords);
        for (int i = 0; i < totalRecords; i++) {
            data.add(bufferPool.getRecord(i));
        }

        // Convert the list to an array for sorting
        short[][] dataArray = data.toArray(new short[0][]);

        // Perform the in-memory quicksort
        quickSortLocal(dataArray, 0, totalRecords - 1);

        // Write the sorted data back to disk
        for (int i = 0; i < dataArray.length; i++) {
            bufferPool.writeRecord(i, dataArray[i]);
        }
    }


    /**
     * Performs the quicksort algorithm on an in-memory array of data.
     * 
     * @param data
     *            The array of data to sort.
     * @param low
     *            The starting index for the sort operation.
     * @param high
     *            The ending index for the sort operation.
     */
    private void quickSortLocal(short[][] data, int low, int high) {
        if (low < high) {
            int pi = partition(data, low, high);
            quickSortLocal(data, low, pi - 1);
            quickSortLocal(data, pi + 1, high);
        }
    }


    /**
     * Partitions the array around a pivot element for the quicksort algorithm.
     * 
     * @param data
     *            The array to partition.
     * @param low
     *            The starting index of the segment to partition.
     * @param high
     *            The ending index of the segment to partition.
     * @return The index of the pivot element after partitioning.
     */
    private int partition(short[][] data, int low, int high) {
        short[] pivot = data[high];
        int i = (low - 1); // Index of smaller element
        for (int j = low; j < high; j++) {
            // If current element is smaller than the pivot
            if (data[j][0] < pivot[0]) {
                i++;
                // Swap data[i] and data[j]
                short[] temp = data[i];
                data[i] = data[j];
                data[j] = temp;
            }
        }
        // Swap data[i+1] and data[high] (or pivot)
        short[] temp = data[i + 1];
        data[i + 1] = data[high];
        data[high] = temp;

        return i + 1;
    }
}
