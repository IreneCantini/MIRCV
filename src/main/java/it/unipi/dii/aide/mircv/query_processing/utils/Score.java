package it.unipi.dii.aide.mircv.query_processing.utils;

import it.unipi.dii.aide.mircv.cli.utils.UploadDataStructures;
import it.unipi.dii.aide.mircv.common.data_structures.*;

import java.io.IOException;

public class Score {
    public static double BM25(String term, Posting p, double k, double b) throws IOException {


        //retrieve term frequency inside the current docid
        int tf = p.getTermFrequency();

        //retrieve the idf
        double idf = UploadDataStructures.Dictionary.get(term).getIdf();

        //compute avg document length
        double avdl = (double)CollectionInfo.getTotal_doc_len()/CollectionInfo.getDocid_counter();

        //retrieve the document lenght of the docid

        return (tf/(k*((1-b) + (b*(UploadDataStructures.Document_Index.get(p.getDocID()).getLength()/avdl))) + tf))*idf;
    }

    public static double TFIDF(String term, Posting p) throws IOException {
        //compute term frequency weight
        double weight_tf = 1 + Math.log10(p.getTermFrequency());

        //retrieve idf
        double idf = UploadDataStructures.Dictionary.get(term).getIdf();

        return weight_tf*idf;
    }

}
