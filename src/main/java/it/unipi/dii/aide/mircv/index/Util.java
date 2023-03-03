package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.basic.data_structures_management.DictionaryElem;
import it.unipi.dii.aide.mircv.basic.data_structures_management.PostingList;
import it.unipi.dii.aide.mircv.basic.data_structures_management.PostingListElem;

import java.util.ArrayList;
import java.util.Comparator;

public class Util {
    /**
     * Funzione per sortare l'array di Dictionary
     * @param list
     */
    public static void sort(ArrayList<DictionaryElem> list) { list.sort(Comparator.comparing(DictionaryElem::getTerm));}

    /**
     * Funzione per sortare l'array di Posting List
     * @param list
     */
    public void sortTerm(ArrayList<PostingList> list) { list.sort(Comparator.comparing(PostingList::getTerm)); }

    public static void freeMemory(){
        SPIMI.listTerm.clear();
        SPIMI.listTermDict.clear();
        SPIMI.positionTerm.clear();
    }
}
