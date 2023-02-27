package it.unipi.dii.aide.mircv.data_structures_management;

import java.util.ArrayList;

public class PostingList {
    private String term;
    private ArrayList<PostingListElem> pl;

    public PostingList(String term, long docID, int f) {
        this.term = term;
        this.pl = new ArrayList<>();
        PostingListElem p = new PostingListElem(docID,f);
        this.pl.add(p);
    }

    public PostingList(String term) {
        this.term=term;
        this.pl=new ArrayList<>();
    }

    public ArrayList<PostingListElem> getPl() {
        return pl;
    }

    public String getTerm() {
        return term;
    }

    public void addPosting(long docID, int f){
        PostingListElem p = new PostingListElem(docID,f);
        this.pl.add(p);
    }

    public long getLastElement(){
        return this.pl.get(this.pl.size() - 1).getDocID();
    }

    public void addPosting(ArrayList<Long> pldoc, ArrayList<Integer> plfreq) {
        while (!pldoc.isEmpty())
            this.addPosting(pldoc.remove(0), plfreq.remove(0));
    }

    public int getSize(){return this.pl.size();}
}
