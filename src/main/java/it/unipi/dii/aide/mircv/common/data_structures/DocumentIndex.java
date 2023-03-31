package it.unipi.dii.aide.mircv.common.data_structures;

import it.unipi.dii.aide.mircv.common.file_management.FileUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Locale;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;

public class DocumentIndex {
    public static  HashMap<Long, DocumentIndexElem> Document_Index = new HashMap<>();

    public static void readDocumentIndexFromDisk() throws IOException {
        //retrieve file channel
        doc_raf = new RandomAccessFile(PATH_TO_DOCUMENT_INDEX, "r");

        DocumentIndexElem d_elem = new DocumentIndexElem();

        for(int i=0; i<CollectionInfo.getDocid_counter(); i+=36){
            d_elem.readDocumentIndexElemFromDisk(i, doc_raf.getChannel());
            Document_Index.put(d_elem.getDocId(), d_elem);
        }
    }
}
