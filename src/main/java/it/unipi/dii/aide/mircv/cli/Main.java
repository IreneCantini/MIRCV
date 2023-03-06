package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.index.FileManagement;
import it.unipi.dii.aide.mircv.index.Merger;
import it.unipi.dii.aide.mircv.index.SPIMI;

import java.io.IOException;

import static it.unipi.dii.aide.mircv.index.Util.formatTime;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        FileManagement fm = new FileManagement();
        SPIMI s = new SPIMI();

        fm.createFileDocumentIndex();
        s.invert();

    }
}
