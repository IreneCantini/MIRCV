package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.basic.text_preprocessing.TextPreprocesser;

import java.io.IOException;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws IOException {
        try {
            TextPreprocesser.executeTextPreprocessing();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
