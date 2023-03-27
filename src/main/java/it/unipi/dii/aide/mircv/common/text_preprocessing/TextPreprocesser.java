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

    //method used for removing Unicode chars during text cleaning
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

    //performs text cleaning
    public static String cleanText(String str){

        //remove URLs
        str = str.replaceAll("https?://\\S+\\s?", " ");

        //reduce to lower case
        str = str.toLowerCase();

        //remove  tag HTML
        str = str.replaceAll("<[^>]*>", "");

        //remove punctuation
        str = str.replaceAll("\\p{Punct}", " ");

        //remove Unicode chars
        str = removeUnicodeChars(str);

        //remove extra whitespaces with a single one
        str = str.replaceAll("\\s+", " ");

        return str;

    }

    public static ArrayList<String> tokenizeLine(String str) {

        return Stream.of(str.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }

    //performs stemming
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

    //performs stopwords removal
    public static ArrayList<String> removeStopwords(ArrayList<String> tokens) throws IOException {
        //open and reading stopwords file
        List<String> stopwords = Files.readAllLines(Paths.get("src/main/resources/stopwords.txt"));

        //remove stopwords
        tokens.removeAll(stopwords);
        return tokens;
    }

    public static ArrayList<String> executeTextPreprocessing(String line) throws IOException {

        ArrayList<String> tokens = new ArrayList<>();

        //Preprocessing Text
        line = cleanText(line);

        //Tokenize
        tokens = tokenizeLine(line);

        //remove stopwords and perform stemming
        if(Flags.isFilter_flag()) {
            tokens = removeStopwords(tokens);
            tokens = stemmingToken(tokens);
        }

        return tokens;

    }
}
