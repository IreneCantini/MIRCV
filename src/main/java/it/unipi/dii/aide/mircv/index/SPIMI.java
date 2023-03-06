package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.basic.data_structures_management.*;
import it.unipi.dii.aide.mircv.basic.text_preprocessing.TextPreprocesser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.index.Util.formatTime;

public class SPIMI {
    /* HashMap <Term, Position> that maintains the position of the term within the two arrays */
    public static HashMap<String, Integer> positionTerm = new HashMap<>();

    /* *** IN-MEMORY STRUCTURES DECLARATION *** */

    /* Array containing the posting lists for each term parsed in the current block */
    public static ArrayList<PostingList> listTerm;

    /* Array containing the information of the terms parsed in the current block */
    public static ArrayList<DictionaryElem> listTermDict;


    /*Variable maintaining the total document's length to compute the average*/
    public double totalLengthDoc=0;

    public SPIMI(){
        listTerm=new ArrayList<>();
        listTermDict= new ArrayList<>();
        /*
        listFileChannelsDict = new ArrayList<>();
        listFileChannelsDoc = new ArrayList<>();
        listFileChannelsFreq = new ArrayList<>();
         */

    }

    /**
     * Legge tutti i documenti e inserimento informazioni in strutture dati in memoria
     */
    public void invert() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        int freq;
        long docid=0;
        String docNo;

        long MaxUsableMememory=Runtime.getRuntime().maxMemory()*5/100;

        //open and read collection
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/collection_prova.tsv"), StandardCharsets.UTF_8));
        String line = reader.readLine();

        while (line != null) {

            ArrayList<String> tokens;
            tokens= TextPreprocesser.executeTextPreprocessing(line);

            totalLengthDoc+=tokens.size();

            docid = docid + 1;
            docNo=tokens.get(0);
            tokens.remove(0);

            FileManagement.writeOneDoc(new DocumentIndexElem(docid, docNo, tokens.size()));

            for (String t: tokens) {
                freq = Collections.frequency(tokens, t);
                //taglio il token a 20 caratteri
                if (t.length()>20)
                    t=t.substring(0,20);

                if (positionTerm.get(t) == null)
                    //il termine non è presente
                    AddTerm(t,docid, freq);
                else
                    //il termine è già presente quindi devo solo aggiungere un posting
                    AddPost(positionTerm.get(t), docid, freq);
            }

            if(Runtime.getRuntime().totalMemory()>MaxUsableMememory) {
                FileManagement.insertOnDisk();
                System.out.println("SONO ARRIVATA AL 80%");
                System.out.println("docID processato: "+docid);

                while (Runtime.getRuntime().totalMemory()>MaxUsableMememory)
                {
                    System.gc();
                    Thread.sleep(100);
                }
            }

            line = reader.readLine();
        }
        FileManagement.insertOnDisk();

        long spimi = System.currentTimeMillis();
        formatTime(start, spimi, "Spimi");

        Merger m = new Merger();
        m.mergeFiles();

        long stop = System.currentTimeMillis();
        formatTime(start, stop, "Merge");

        Util.printII(FileManagement.getDict(), FileManagement.getIIDoc(), FileManagement.getIIFreq(), null);
    }

    /**
     * Aggiungo termine nuovo all'array di posting list e all'array del dizionario
     * @param t
     * @param docId
     * @param freq
     */
    private void AddTerm(String t, long docId, int freq) {

        this.listTerm.add(new PostingList(t, docId, freq));
        this.listTermDict.add(new DictionaryElem(t, 1, freq));
        this.positionTerm.put(t, this.listTerm.size()-1);
    }

    /**
     * Aggiunge un nuovo posting al termine già presente nell'array
     * @param pos
     * @param docid
     * @param freq
     */
    public void AddPost(Integer pos, long docid, Integer freq) {

        PostingList postingList_T = listTerm.get(pos);

        //se è uguale significa che era già presente in questo documento e ho già salvato tutte le informazioni
        if (postingList_T.getLastElement() != docid) {
            listTerm.get(pos).addPosting(docid, freq);
            listTermDict.get(pos).incCollectionFrequency(freq);
            listTermDict.get(pos).incDocumentFrequency();
        }
    }
}
