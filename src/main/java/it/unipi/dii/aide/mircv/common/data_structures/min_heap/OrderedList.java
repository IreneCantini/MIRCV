package it.unipi.dii.aide.mircv.common.data_structures.min_heap;
public class OrderedList {

    String term;
    int index;

    public OrderedList(String term, int index) {
        this.term = term;
        this.index = index;
    }

    public String getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setIndex(int index) {
        this.index = index;
    }

}
