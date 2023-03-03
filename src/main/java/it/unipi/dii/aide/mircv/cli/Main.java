package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.basic.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.index.FileMenagement;
import it.unipi.dii.aide.mircv.index.SPIMI;

import java.io.IOException;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        FileMenagement fm=new FileMenagement();
        SPIMI obj=new SPIMI();
        fm.createFileDocumentIndex();
        obj.invert();
    }
}
