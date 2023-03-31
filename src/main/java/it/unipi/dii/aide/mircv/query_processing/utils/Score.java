package it.unipi.dii.aide.mircv.query_processing.utils;

import it.unipi.dii.aide.mircv.common.data_structures.*;

import java.io.IOException;

public class Score {
    public static double BM25(String term, Posting p, double k, double b) throws IOException {

        //TODO: valutare la possibilità di utilizzare la term frequency pesata
        //retrieve term frequency inside the current docid
        int tf = p.getTermFrequency();
        //double weight_tf = 1 + Math.log10(p.getTermFrequency());

        //retrieve the idf
        DictionaryElem dictionary_elem = QueryUtils.dictionaryBinarySearch(term);
        double idf = dictionary_elem.getIdf();

        //compute avg document length
        double avdl = (double)CollectionInfo.getTotal_doc_len()/CollectionInfo.getDocid_counter();

        //retrieve the document lenght of the docid
        DocumentIndexElem d_elem = QueryUtils.documentBinarySearch(p.getDocID());

        return (tf/(k*((1-b) + (b*(d_elem.getLength()/avdl))) + tf))*idf;
    }

    public static double TFIDF(String term, Posting p) throws IOException {
        //compute term frequency weight
        double weight_tf = 1 + Math.log10(p.getTermFrequency());

        //retrieve idf
        DictionaryElem dictionary_elem = QueryUtils.dictionaryBinarySearch(term);
        double idf = dictionary_elem.getIdf();

        return weight_tf*idf;
    }

}
