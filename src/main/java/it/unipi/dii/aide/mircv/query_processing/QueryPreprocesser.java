package it.unipi.dii.aide.mircv.query_processing;

import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT.*;

public class QueryPreprocesser {

    public static ArrayList<PostingList> plQueryTerm;

    public static void executeQueryProcesser(ArrayList<String> tokens) throws IOException, InterruptedException {
        PostingList pl=new PostingList();
        for(String t: tokens){
            pl.setTerm(t);
            pl.obtainPostingList(t);
            plQueryTerm.add(pl);
            pl.getPl().clear();
        }

        executeDAAT(10);
    }
}
