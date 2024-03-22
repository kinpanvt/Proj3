import java.io.*;
import java.util.*;

/**
 * Designated file type
 * 
 * @author kinjalpandey, architg03
 * @version 03/22/2024
 */
enum FileType {
    /**
     * Represents a binary file type. Binary files are typically not
     * human-readable and
     * may require specific software for reading or processing.
     */
    BINARY,

    /**
     * Represents an ASCII (text) file type. ASCII files are text files that
     * contain
     * human-readable characters and can be opened with most text editors.
     */
    ASCII
};




/**
 * Generate a test data file of records. Each record is 4 bytes: 2 bytes for
 * the key (a java short, used for sorting), 2 bytes for the value (a java
 * short). A group of 2048 records is a block. Depending on the method, you can
 * generate two types of files: ASCII or raw binary shorts. In ASCII mode, the
 * records are constrained to specific values which align with specific ASCII
 * values. Reading this file as text/ascii will show a record in this format:
 * [space][letter][space][space]. In Binary mode, the keys and values of a
 * record are in the range [1-30000). Raw binary is not easily human-readable,
 * and often looks like textual garbage.
 * 
 * @author Cliff Shaffer, Patrick Sullivan
 * 
 * @version 03/22/2024
 */
public class FileGenerator {

    /**
     * The number of bytes that represent a key in the system. This constant is
     * set to the size of a {@code Short},
     * implying that keys are expected to be short integer values. The size of a
     * short in Java is typically 2 bytes.
     */
    static public final int BYTES_IN_KEY = Short.BYTES;

    /**
     * The number of bytes that represent a value in the system. Similar to
     * {@code BYTES_IN_KEY}, this is set to
     * the size of a {@code Short}, indicating that values are also short
     * integer values, typically requiring 2 bytes.
     */
    static public final int BYTES_IN_VALUE = Short.BYTES;

    /**
     * The total number of bytes used by a single record in the system. A record
     * consists of a key-value pair,
     * therefore this size is the sum of {@code BYTES_IN_KEY} and
     * {@code BYTES_IN_VALUE}. This constant defines
     * the size of a record for storage and processing purposes.
     */
    static public final int BYTES_PER_RECORD = BYTES_IN_KEY + BYTES_IN_VALUE;

    /**
     * Defines the number of records that can be stored in a single block of
     * memory or storage. This constant
     * is essential for calculating the allocation and management of blocks in
     * the system's storage architecture.
     */
    static public final int RECORDS_PER_BLOCK = 1024;

    /**
     * The total number of bytes that a single block occupies, calculated as the
     * product of {@code RECORDS_PER_BLOCK}
     * and {@code BYTES_PER_RECORD}. This constant is crucial for understanding
     * the storage footprint of blocks
     * within the system.
     */
    static public final int BYTES_PER_BLOCK = RECORDS_PER_BLOCK
        * BYTES_PER_RECORD;

    private final int numBlocks;

    /**
     * The name of the file associated with this instance. This could represent
     * a data file, configuration file,
     * or any file that the system interacts with. {@code fname} provides a
     * means to link the instance with its
     * external data representation or storage mechanism.
     */
    private final String fname;

    private Random rng;

    /**
     * Creates a FileGenerator object for making random files of data.
     * 
     * @param fname
     *            the file name (example 'oneBlock.txt' or 'data.bin')
     * @param numBlocks
     *            number of blocks of data in the file. each block is
     */
    public FileGenerator(String fname, int numBlocks) {
        this.numBlocks = numBlocks;
        this.fname = fname;
        rng = new Random();
    }


    /**
     * [Optional] Sets the rng seed to make generation deterministic instead of
     * random. Files generated using the same seed will be exactly the same. Can
     * be helpful for consistent testing.
     * 
     * @param seed
     *            the seed
     */
    public void setSeed(long seed) {
        rng.setSeed(seed);
    }


    /**
     * Generates a file using the given setup.
     * 
     * @param ft
     *            type of file being generated, either binary or ASCII
     */
    public void generateFile(FileType ft) {
        DataOutputStream dos;
        try {
            dos = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(fname)));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("ERROR: File not found. See System.err");
            return; // exit method early, dos is unusable already
        }

        try {
            if (ft == FileType.ASCII)
                generateAsciiFile(dos);
            else if (ft == FileType.BINARY)
                generateBinaryFile(dos);

            dos.flush(); // flush any contents stuck in buffer to file.
            dos.close(); // close file when done generating
        }
        catch (IOException e) {
            e.printStackTrace();
            System.out.println("ERROR: IOException in fileGen. See System.err");
            return;
        }
    }


    /**
     * Generates a file of random ASCII records. This is just to make it easier
     * for your visual inspection
     * Record keys are one space ' ' and a randomly ASCII charachter within in
     * the range ' A' to ' Z' .
     * Record values are always two spaces' '.
     * 
     * @param dos
     *            The data output stream to write data to
     * @throws IOException
     *             if writing shorts encounters an issue
     */
    private void generateAsciiFile(DataOutputStream dos) throws IOException {
        int randKey;
        short blankVal = 8224; // raw binary data representing a double-space
        int asciiOffset = 8257; // offset to reach ascii range starting at ' A'
        int range = 26; // number of characters in alphabet range, from A to Z.

        for (int i = 0; i < numBlocks; i++) {
            for (int j = 0; j < RECORDS_PER_BLOCK; j++) {
                randKey = Math.abs(rng.nextInt() % range) + asciiOffset;
                dos.writeShort(randKey); // THIS writes to the file!
                dos.writeShort(blankVal); // THIS writes to the file!
            }
        }
    }


    /**
     * Generates a file of random binary records. Record keys and values are
     * shorts in range [1-30000)
     * 
     * @param dos
     *            The data output stream to write data to
     * @throws IOException
     *             if writing shorts encounters an issue
     */
    private void generateBinaryFile(DataOutputStream dos) throws IOException {
        int randKey;
        int randVal;
        int minRand = 1; // minimum random short
        int range = 30000 - minRand; // max random short - min random short
        for (int i = 0; i < numBlocks; i++) {
            for (int j = 0; j < RECORDS_PER_BLOCK; j++) {
                // val = (short)(random(29999) + 1);
                randKey = Math.abs(rng.nextInt() % range) + minRand;
                randVal = Math.abs(rng.nextInt() % range) + minRand;
                dos.writeShort((short)randKey); // THIS writes to the file!
                dos.writeShort((short)randVal); // THIS writes to the file!
            }
        }
    }
}
