package it.unipi.dii.aide.mircv.basic.data_structures_management.min_heap;

public class Comparator implements java.util.Comparator<OrderedList> {
    @Override
    public int compare(OrderedList o1, OrderedList o2) {
        if (o1.getTerm().compareTo(o2.getTerm()) == 0) {
            if(o1.getIndex()<o2.getIndex())
                return -1;
            else
                return 1;
        } else {
            return o1.getTerm().compareTo(o2.getTerm());
        }
    }
}
