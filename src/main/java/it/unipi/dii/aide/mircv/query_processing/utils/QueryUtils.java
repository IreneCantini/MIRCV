package it.unipi.dii.aide.mircv.query_processing.utils;

import it.unipi.dii.aide.mircv.common.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.common.data_structures.DocumentIndexElem;
import it.unipi.dii.aide.mircv.index.SPIMI;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.RandomAccessFile_map;
import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.doc_raf;

public class QueryUtils {

    private DictionaryElem dictionaryBinarySearch(String term) throws IOException {

        // Variabili per mantenere gli estremi della ricerca e la posizione centrale
        long low = 0;
        long high = (RandomAccessFile_map.get(SPIMI.block_number+1).get(0).getChannel().size() / 92); // Mantiene quanti termini sono presenti nel dizionario
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
            mappedByteBuffer = RandomAccessFile_map.get(SPIMI.block_number+1).get(0).getChannel().map(FileChannel.MapMode.READ_ONLY, mid*92, 20);

            if (mappedByteBuffer != null) {
                d.setTerm(StandardCharsets.UTF_8.decode(mappedByteBuffer).toString());
            }

            //System.out.println("Termine letto:" + d.getTerm());
            temp = d.getTerm().replaceAll("\\s", "");

            if (temp.compareTo(term) < 0) {
                low = mid + 1;
            } else if (temp.compareTo(term) > 0) {
                high = mid - 1;
            } else {
                // Termine trovato
                d.readDictionaryElemFromDisk(mid*92, RandomAccessFile_map.get(SPIMI.block_number+1).get(0).getChannel());

                return d; // ritorna l'oggetto dizionario relativo a quel termine
            }
        }
        return null;
    }
    private DocumentIndexElem documentBinarySearch(long docID) throws IOException {

        // Variabili per mantenere gli estremi della ricerca e la posizione centrale
        long low = 0;
        long high = (doc_raf.getChannel().size()-8 )/ 36; // Mantiene quanti documenti sono presenti nel document index
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
            mappedByteBuffer = doc_raf.getChannel().map(FileChannel.MapMode.READ_ONLY, mid*36, 8);

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
}
