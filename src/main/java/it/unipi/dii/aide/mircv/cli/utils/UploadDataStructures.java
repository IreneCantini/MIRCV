package it.unipi.dii.aide.mircv.cli.utils;

import it.unipi.dii.aide.mircv.common.data_structures.CollectionInfo;
import it.unipi.dii.aide.mircv.common.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.common.data_structures.DocumentIndexElem;
import it.unipi.dii.aide.mircv.common.data_structures.Flags;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;

public class UploadDataStructures {

    public static HashMap<Long, DocumentIndexElem> Document_Index = new HashMap<>();
    public static HashMap<String, DictionaryElem> Dictionary = new HashMap<>();

    public static void readDocumentIndexFromDisk() throws IOException {
        //retrieve file channel
        doc_raf = new RandomAccessFile(PATH_TO_DOCUMENT_INDEX, "r");

        DocumentIndexElem d_elem;

        for(int i = 0; i< doc_raf.getChannel().size(); i+=36){
            d_elem = new DocumentIndexElem();
            d_elem.readDocumentIndexElemFromDisk(i, doc_raf.getChannel());
            Document_Index.put(d_elem.getDocId(), d_elem);
        }
    }

    public static void readDictionaryFromDisk() throws IOException {
        //retrieve file channel
        RandomAccessFile dic_raf = new RandomAccessFile(PATH_TO_VOCABULARY, "r");

        DictionaryElem d_elem;

        for(int i = 0; i < dic_raf.getChannel().size(); i+=92){
            d_elem = new DictionaryElem();
            d_elem.readDictionaryElemFromDisk(i, dic_raf.getChannel());
            Dictionary.put(d_elem.getTerm(), d_elem);
        }
    }

    public static void readFlagsFromDisk() throws IOException {
        //retrieve file channel
        Flags_raf = new RandomAccessFile(PATH_TO_FLAGS_FILE, "r");

        ByteBuffer FlagsBuffer = ByteBuffer.allocate(16);

        Flags_raf.getChannel().position(0);


        while (FlagsBuffer.hasRemaining()){
            Flags_raf.getChannel().read(FlagsBuffer);
        }

        FlagsBuffer.rewind();

        Flags.setCompression_flag(FlagsBuffer.getInt() == 1);

        Flags.setFilter_flag(FlagsBuffer.getInt() == 1);

        Flags.setMaxScore_flag(FlagsBuffer.getInt() == 1);

        Flags.setDebug_flag(FlagsBuffer.getInt() == 1);
    }

    public static void readCollectionInfoFromDisk() throws IOException {
        CollectionInfo_raf = new RandomAccessFile(PATH_TO_COLLECTION_INFO_FILE, "r");

        ByteBuffer docIndexBuffer = ByteBuffer.allocate(16);

        CollectionInfo_raf.getChannel().position(0);

        while (docIndexBuffer.hasRemaining()){
            CollectionInfo_raf.getChannel().read(docIndexBuffer);
        }

        docIndexBuffer.rewind();
        CollectionInfo.setDocid_counter(docIndexBuffer.getLong());
        CollectionInfo.setTotal_doc_len(docIndexBuffer.getLong());
    }
}
