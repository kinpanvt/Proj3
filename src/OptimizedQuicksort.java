import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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

        // Perform the iterative quicksort
        quickSortIterative(dataArray, 0, totalRecords - 1);

        // Write the sorted data back to disk
        for (int i = 0; i < dataArray.length; i++) {
            bufferPool.writeRecord(i, dataArray[i]);
        }
    }


    private void quickSortIterative(short[][] data, int low, int high) {
        Stack<Integer> stack = new Stack<>();
        stack.push(low);
        stack.push(high);

        while (!stack.isEmpty()) {
            high = stack.pop();
            low = stack.pop();

            if (low < high) {
                short[] pivot = data[low];

                // 3-way partitioning
                int[] partitionIndices = threeWayPartition(data, low, high,
                    pivot);
                int lt = partitionIndices[0];
                int gt = partitionIndices[1];

                // Push left segment onto stack if it has more than one element
                if (lt - 1 > low) {
                    stack.push(low);
                    stack.push(lt - 1);
                }

                // Push right segment onto stack if it has more than one element
                if (gt + 1 < high) {
                    stack.push(gt + 1);
                    stack.push(high);
                }
            }
        }

    }


    private int[] threeWayPartition(
        short[][] data,
        int low,
        int high,
        short[] pivot) {
        int lt = low;
        int gt = high;
        int i = low + 1;
        while (i <= gt) {
            int cmp = compare(data[i], pivot);
            if (cmp < 0)
                swap(data, lt++, i++);
            else if (cmp > 0)
                swap(data, i, gt--);
            else
                i++;
        }
        return new int[] { lt, gt };
    }


    private void swap(short[][] data, int i, int j) {
        short[] temp = data[i];
        data[i] = data[j];
        data[j] = temp;
    }


    private int compare(short[] a, short[] b) {
        return Short.compare(a[0], b[0]);
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
        int i = (low - 1);
        for (int j = low; j < high; j++) {
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


    /**
     * Performs insertion sort on a portion of the array.
     * 
     * @param data
     *            The array of data to sort.
     * @param low
     *            The starting index for the sort operation.
     * @param high
     *            The ending index for the sort operation.
     */
    private void insertionSort(short[][] data, int low, int high) {
        for (int i = low + 1; i <= high; i++) {
            short[] key = data[i];
            int j = i - 1;
            while (j >= low && data[j][0] > key[0]) {
                data[j + 1] = data[j];
                j = j - 1;
            }
            data[j + 1] = key;
        }
    }

}
