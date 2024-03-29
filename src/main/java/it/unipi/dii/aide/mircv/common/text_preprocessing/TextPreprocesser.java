package it.unipi.dii.aide.mircv.common.text_preprocessing;

import it.unipi.dii.aide.mircv.common.data_structures.Flags;
import org.tartarus.snowball.ext.PorterStemmer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TextPreprocesser {

    public static List<String> stopwords_global;

    /**
     * Function that is used for removing Unicode chars during text cleaning
     * @param s is the string to preprocess
     * @return the string pre-processed
     */
    private static String removeUnicodeChars(String s) {

        String str;
        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);

        str = new String(strBytes, StandardCharsets.UTF_8);

        Pattern unicodeOutliers = Pattern.compile("[^\\x00-\\x7F]",
                Pattern.UNICODE_CASE | Pattern.CANON_EQ
                        | Pattern.CASE_INSENSITIVE);

        Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(str);
        str = unicodeOutlierMatcher.replaceAll(" ");

        return str;
    }

    /**
     * Performs the general pre-processing phase
     * @param str is the string to preprocess
     * @return the string pre-processed
     */
    public static String cleanText(String str){

        /* Remove URLs */
        str = str.replaceAll("https?://\\S+\\s?", " ");

        /* Reduce to lower case */
        str = str.toLowerCase();

        /* Remove HTML tags */
        str = str.replaceAll("<[^>]*>", "");

        /* Remove punctuation */
        str = str.replaceAll("\\p{Punct}", " ");

        /* Remove Unicode chars */
        str = removeUnicodeChars(str);

        /* Remove extra whitespaces with a single one */
        str = str.replaceAll("\\s+", " ");

        return str;
    }

    /**
     * Function that tokenize a string into multiple tokens
     * @param str is the string to preprocess
     * @return an arrayList of tokens
     */
    public static ArrayList<String> tokenizeLine(String str) {

        return Stream.of(str.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }

    /**
     * Function that performs the stemming
     * @param tokens array of tokens
     * @return the array "stemmed"
     */
    public static ArrayList<String> stemmingToken(ArrayList<String> tokens) {

        PorterStemmer ps = new PorterStemmer();
        ArrayList<String> newList = new ArrayList<>();

        for (String eachTok : tokens) {
            ps.setCurrent(eachTok);
            ps.stem();
            eachTok = ps.getCurrent();
            newList.add(eachTok);
        }

        return newList;
    }

    /**
     * Perform the stop-words removal
     * @param tokens array of tokens
     * @return tokens without the stop-words
     * @throws IOException if the channel is not found
     */
    public static ArrayList<String> removeStopwords(ArrayList<String> tokens) throws IOException {

        //List<String> stopwords = Files.readAllLines(Paths.get("src/main/resources/stopwords.txt"));
        tokens.removeAll(stopwords_global);

        return tokens;
    }

    public static ArrayList<String> executeTextPreprocessing(String line) throws IOException {

        ArrayList<String> tokens;

        /* Preprocessing Text */
        line = cleanText(line);

        tokens = tokenizeLine(line);

        /* Remove stop-words and perform stemming */
        if (Flags.isFilter_flag()) {
            tokens = removeStopwords(tokens);
            tokens = stemmingToken(tokens);
        }

        return tokens;
    }
}
