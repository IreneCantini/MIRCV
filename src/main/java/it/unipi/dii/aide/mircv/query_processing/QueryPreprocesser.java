package it.unipi.dii.aide.mircv.query_processing;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.query_processing.Algorithms.DAAT.*;
import static it.unipi.dii.aide.mircv.query_processing.Algorithms.MaxScore.*;

public class QueryPreprocesser {

    public static ArrayList<PostingList> plQueryTerm = new ArrayList<>();
    //public static HashMap<Double, PostingList> score_hm = new HashMap<>();
    public static ArrayList<Double> maxScoreQueryTerm = new ArrayList<>();

    public static void executeQueryProcesser(ArrayList<String> tokens) throws IOException, InterruptedException {

        for(String t: tokens){
            PostingList pl=new PostingList();
            pl.getPl().clear();
            pl.setTerm(t);
            pl.obtainPostingList(t);
            if(!Flags.isMaxScore_flag()){
                plQueryTerm.add(pl);
            }
        }

        if(Flags.isMaxScore_flag())
            executeMaxScore(10);
        else
            executeDAAT(10);

        plQueryTerm.clear();
        //score_hm.clear();
    }


}
