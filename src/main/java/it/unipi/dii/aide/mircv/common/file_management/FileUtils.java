package it.unipi.dii.aide.mircv.common.file_management;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.index.SPIMI.block_number;

public class FileUtils {
    //Total number of documents in the collection
    public static long collection_length = 0;

    //partial PATH of the temporary docid posting lists file
    public static final String PATH_TO_PARTIAL_DOCIDS_POSTINGLIST = "src/main/resources/docid_pl_tmp_";

    //partial PATH of the temporary freq posting lists file
    public static final String PATH_TO_PARTIAL_FREQ_POSTINGLIST = "src/main/resources/freq_pl_tmp_";

    //partial PATH of the temporary vocabulary file
    public static final String PATH_TO_PARTIAL_VOCABULARY = "src/main/resources/vocabulary_tmp_";

    //PATH of the docid posting lists file
    public static final String PATH_TO_DOCIDS_POSTINGLIST = "src/main/resources/docid_posting_lists";

    //PATH of the freq posting lists file
    public static final String PATH_TO_FREQ_POSTINGLIST = "src/main/resources/freq_posting_lists";

    //PATH of the vocabulary file
    public static final String PATH_TO_VOCABULARY = "src/main/resources/vocabulary";

    //PATH of the Document Index file
    public static final String PATH_TO_DOCUMENT_INDEX = "src/main/resources/document_index";

    //PATH of the Skipping file
    public static final String PATH_TO_SKIPPING_FILE = "src/main/resources/skipping";

    //PATH of the Collection Info file
    public static final String PATH_TO_COLLECTION_INFO_FILE = "src/main/resources/collection_info";

    //PATH of the Flags file
    public static final String PATH_TO_FLAGS_FILE = "src/main/resources/flags";

    public static RandomAccessFile doc_raf;

    public static RandomAccessFile skippingBlock_raf;

    public static RandomAccessFile CollectionInfo_raf;

    public static RandomAccessFile Flags_raf;

    public static final HashMap<Integer, ArrayList<RandomAccessFile>> RandomAccessFile_map = new HashMap<>();


    public static void createTemporaryFile() throws FileNotFoundException {
        ArrayList<RandomAccessFile> raf_array = new ArrayList<>();

        //create dictionary File and get the corresponding file channel
        raf_array.add(new RandomAccessFile(new File(PATH_TO_PARTIAL_VOCABULARY + block_number), "rw"));

        //create dictionary File and get the corresponding file channel
        raf_array.add(new RandomAccessFile(new File(PATH_TO_PARTIAL_DOCIDS_POSTINGLIST + block_number), "rw"));

        //create dictionary File and get the corresponding file channel
        raf_array.add(new RandomAccessFile(new File(PATH_TO_PARTIAL_FREQ_POSTINGLIST + block_number), "rw"));

        RandomAccessFile_map.put(block_number, raf_array);
    }

    public static void createFinalFile() throws FileNotFoundException {
        ArrayList<RandomAccessFile> raf_array = new ArrayList<>();

        //create dictionary File
        raf_array.add(new RandomAccessFile(new File(PATH_TO_VOCABULARY), "rw"));

        //create dictionary File
        raf_array.add(new RandomAccessFile(new File(PATH_TO_DOCIDS_POSTINGLIST), "rw"));

        //create dictionary File
        raf_array.add(new RandomAccessFile(new File(PATH_TO_FREQ_POSTINGLIST), "rw"));

        //create Skipping file
        skippingBlock_raf = new RandomAccessFile(new File(PATH_TO_SKIPPING_FILE), "rw");

        //create Collection Info file
        CollectionInfo_raf = new RandomAccessFile(new File(PATH_TO_COLLECTION_INFO_FILE),"rw");

        //create Flags file
        Flags_raf = new RandomAccessFile(new File(PATH_TO_FLAGS_FILE),"rw");

        RandomAccessFile_map.put(block_number + 1, raf_array);
    }

    public static void createDocIndexFile() throws IOException {
        doc_raf = new RandomAccessFile(new File(PATH_TO_DOCUMENT_INDEX), "rw");
    }

    public static void deleteFile(String path) throws InterruptedException {
        File myFile = new File(path);
        while(!myFile.delete()){
            //System.out.println("Failed to delete the file: " + myFile.getName() +". Retry to delete it!");
            System.gc();
            Thread.sleep(100);
        }
    }

    public static void deleteTemporaryFile() throws InterruptedException {
        for(int i = 0; i< block_number+1; i++){
            FileUtils.deleteFile(PATH_TO_PARTIAL_VOCABULARY + i);
            FileUtils.deleteFile(PATH_TO_PARTIAL_DOCIDS_POSTINGLIST + i);
            FileUtils.deleteFile(PATH_TO_PARTIAL_FREQ_POSTINGLIST + i);
        }
    }
}

