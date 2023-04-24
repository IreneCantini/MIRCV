package it.unipi.dii.aide.mircv.common.file_management;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.index.SPIMI.block_number;

public class FileUtils {

    /* Total number of documents in the collection */
    public static long collection_length = 0;

    /* Partial Path of the temporary docIDs posting lists file */
    public static final String PATH_TO_PARTIAL_DOCIDS_POSTINGLIST = "src/main/resources/docid_pl_tmp_";

    /* Partial Path of the temporary frequencies posting lists file */
    public static final String PATH_TO_PARTIAL_FREQ_POSTINGLIST = "src/main/resources/freq_pl_tmp_";

    /* Partial Path of the temporary vocabulary file */
    public static final String PATH_TO_PARTIAL_VOCABULARY = "src/main/resources/vocabulary_tmp_";

    /* Path of the docID posting lists file */
    public static final String PATH_TO_DOCIDS_POSTINGLIST = "src/main/resources/docid_posting_lists";

    /* Path of the frequencies posting lists file */
    public static final String PATH_TO_FREQ_POSTINGLIST = "src/main/resources/freq_posting_lists";

    /* Path of the Vocabulary file */
    public static final String PATH_TO_VOCABULARY = "src/main/resources/vocabulary";

    /* Path of the Document Index file */
    public static final String PATH_TO_DOCUMENT_INDEX = "src/main/resources/document_index";

    /* Path of the Skipping file */
    public static final String PATH_TO_SKIPPING_FILE = "src/main/resources/skipping";

    /* Path of the Collection Information file */
    public static final String PATH_TO_COLLECTION_INFO_FILE = "src/main/resources/collection_info";

    /* Path of the Flags file */
    public static final String PATH_TO_FLAGS_FILE = "src/main/resources/flags";

    public static RandomAccessFile doc_raf;

    public static RandomAccessFile skippingBlock_raf;

    public static RandomAccessFile CollectionInfo_raf;

    public static RandomAccessFile Flags_raf;

    public static final HashMap<Integer, ArrayList<RandomAccessFile>> RandomAccessFile_map = new HashMap<>();


    /**
     * Function that creates the temporary files to contain the partial structures
     * @throws FileNotFoundException if the file is not found
     */
    public static void createTemporaryFile() throws FileNotFoundException {

        ArrayList<RandomAccessFile> raf_array = new ArrayList<>();

        /* Create Dictionary file and get the corresponding FileChannel */
        raf_array.add(new RandomAccessFile(new File(PATH_TO_PARTIAL_VOCABULARY + block_number), "rw"));

        /* Create docIDs posting list file and get the corresponding FileChannel */
        raf_array.add(new RandomAccessFile(new File(PATH_TO_PARTIAL_DOCIDS_POSTINGLIST + block_number), "rw"));

        /* Create frequencies file and get the corresponding FileChannel */
        raf_array.add(new RandomAccessFile(new File(PATH_TO_PARTIAL_FREQ_POSTINGLIST + block_number), "rw"));

        RandomAccessFile_map.put(block_number, raf_array);
    }

    public static void createFinalFile() throws FileNotFoundException {

        ArrayList<RandomAccessFile> raf_array = new ArrayList<>();

        /* Create Dictionary file */
        raf_array.add(new RandomAccessFile(new File(PATH_TO_VOCABULARY), "rw"));

        /* Create docIDs posting list file */
        raf_array.add(new RandomAccessFile(new File(PATH_TO_DOCIDS_POSTINGLIST), "rw"));

        /* Create frequencies posting list file */
        raf_array.add(new RandomAccessFile(new File(PATH_TO_FREQ_POSTINGLIST), "rw"));

        /* Create Skipping file */
        skippingBlock_raf = new RandomAccessFile(new File(PATH_TO_SKIPPING_FILE), "rw");

        /* Create Collection Info file */
        CollectionInfo_raf = new RandomAccessFile(new File(PATH_TO_COLLECTION_INFO_FILE),"rw");

        /* Create Flags file */
        Flags_raf = new RandomAccessFile(new File(PATH_TO_FLAGS_FILE),"rw");

        RandomAccessFile_map.put(block_number + 1, raf_array);
    }

    public static void createDocIndexFile() throws IOException {
        doc_raf = new RandomAccessFile(new File(PATH_TO_DOCUMENT_INDEX), "rw");
    }

    /**
     * Function that delete the file present on disk
     * @param path is the complete path to the file
     * @throws InterruptedException
     */
    public static void deleteFile(String path) throws InterruptedException {
        File myFile = new File(path);

        while (!myFile.delete()) {
            System.gc();
            Thread.sleep(100);
        }
    }

    /**
     * Delete all temporary files
     * @throws InterruptedException
     */
    public static void deleteTemporaryFile() throws InterruptedException {

        for (int i = 0; i < block_number + 1; i++){

            FileUtils.deleteFile(PATH_TO_PARTIAL_VOCABULARY + i);
            FileUtils.deleteFile(PATH_TO_PARTIAL_DOCIDS_POSTINGLIST + i);
            FileUtils.deleteFile(PATH_TO_PARTIAL_FREQ_POSTINGLIST + i);
        }
    }
}

