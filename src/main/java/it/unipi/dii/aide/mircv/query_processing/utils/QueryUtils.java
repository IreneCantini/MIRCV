package it.unipi.dii.aide.mircv.query_processing.utils;

import it.unipi.dii.aide.mircv.common.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.common.data_structures.DocumentIndexElem;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;
import static it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser.plQueryTerm;

public class QueryUtils {

    public static DictionaryElem dictionaryBinarySearch(String term) throws IOException {
        //retrieve the file channel to the dictionary file
        RandomAccessFile Dictionary_raf = new RandomAccessFile(PATH_TO_VOCABULARY, "r");


        // Variabili per mantenere gli estremi della ricerca e la posizione centrale
        long low = 0;
        long high = (Dictionary_raf.getChannel().size() / 92); // Mantiene quanti termini sono presenti nel dizionario
        long mid;

        // Variabili di appoggio temporaneo
        MappedByteBuffer mappedByteBuffer;
        String temp;

        // Variabile per mantenere l'oggeto Dictionary da ritornare
        DictionaryElem d = new DictionaryElem();

        while (low <= high) {

            //System.out.println("low: "+low+", high: "+high);
            mid = (low + high) / 2;

            // get term
            mappedByteBuffer = Dictionary_raf.getChannel().map(FileChannel.MapMode.READ_ONLY, mid*92, 20);

            if (mappedByteBuffer != null) {
                d.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());
            }

            //System.out.println("Termine letto:" + d.getTerm());
            temp = d.getTerm().replaceAll("\\s", "").trim();

            if (temp.compareTo(term) < 0) {
                low = mid + 1;
            } else if (temp.compareTo(term) > 0) {
                high = mid - 1;
            } else {
                // Termine trovato
                d.readDictionaryElemFromDisk(mid*92, Dictionary_raf.getChannel());

                return d; // ritorna l'oggetto dizionario relativo a quel termine
            }
        }
        return null;
    }
    public static DocumentIndexElem documentBinarySearch(long docID) throws IOException {

        //retrieve the File Channel to the Document Index file
        doc_raf = new RandomAccessFile(PATH_TO_DOCUMENT_INDEX, "r");

        // Variabili per mantenere gli estremi della ricerca e la posizione centrale
        long low = 0;
        long high = (doc_raf.getChannel().size())/ 36; // Mantiene quanti documenti sono presenti nel document index
        long mid;

        // Variabili di appoggio temporaneo
        MappedByteBuffer mappedByteBuffer;
        String temp;

        // Variabile per mantenere l'oggeto Dictionary da ritornare
        DocumentIndexElem d = new DocumentIndexElem();

        while (low <= high) {

            //System.out.println("low: "+low+", high: "+high);
            mid = (low + high) / 2;

            // get term
            mappedByteBuffer = doc_raf.getChannel().map(FileChannel.MapMode.READ_ONLY, (mid*36) + 20, 8);

            if (mappedByteBuffer != null) {
                d.setDocId(mappedByteBuffer.getLong());
            }

            if (d.getDocId() < docID) {
                low = mid + 1;
            } else if (d.getDocId() > docID) {
                high = mid - 1;
            } else {
                // Termine trovato
                d.readDocumentIndexElemFromDisk((int) (mid*36), doc_raf.getChannel());

                return d; // ritorna l'oggetto dizionario relativo a quel termine
            }
        }
        return null;
    }

    public static long minimumDocID(){
        ArrayList<Long> docid_candidate = new ArrayList<Long>();

        for(PostingList pl: plQueryTerm){
            docid_candidate.add(pl.getPl().get(0).getDocID());
        }

        return Collections.min(docid_candidate);
    }


}
