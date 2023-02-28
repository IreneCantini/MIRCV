package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.basic.data_structures_management.*;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;

public class SPIMI {
    /* HashMap <Term, Position> that maintains the position of the term within the two arrays */
    HashMap<String, Integer> positionTerm = new HashMap<>();


    /* *** FILE CHANNEL VARIABLES *** */

    /* Channel connected to file containing the document information */
    private FileChannel fileChannelDocIndex;

    /* Channel connected to file containing the (new) dictionary file */
    private FileChannel fileChannelDict_new;

    /* Channel connected to file containing the (new, maybe updated) posting lists for each term */
    private FileChannel fileChannelII_doc_new;

    /* Channel connected to file containing the (new, maybe updated) frequencies for each term */
    private FileChannel fileChannelII_freq_new;

    private FileChannel fileChannel_skipping;

    /* *** ARRAY CONTAINING FILE CHANNEL OF TEMPORARY FILES *** */
    private ArrayList<FileChannel> listFileChannelsDict;

    private ArrayList<FileChannel> listFileChannelsDoc;

    private ArrayList<FileChannel> listFileChannelsFreq;

    /* *** IN-MEMORY STRUCTURES DECLARATION *** */

    /* Array containing the posting lists for each term parsed in the current block */
    private ArrayList<PostingList> listTerm;

    /* Array containing the information of the terms parsed in the current block */
    private ArrayList<DictionaryElem> listTermDict;

    /*Variable maintaining the total document's length to compute the average*/
    public double totalLengthDoc=0;

    public SPIMI(){
        listTerm=new ArrayList<>();
        listTermDict= new ArrayList<>();
        listFileChannelsDict = new ArrayList<>();
        listFileChannelsDoc = new ArrayList<>();
        listFileChannelsFreq = new ArrayList<>();

    }

    public void createFileDocumentIndex() throws IOException {
        String filePath = "src/main/resources/document_index.dat";
        // Inizializziamo il file
        File file = new File(filePath);
        if(!file.createNewFile()){
            System.out.println("Non è stato possibile creare il file del document index");
        }
        else{
            Path path_docI = Path.of("src/main/resources/document_index.dat");

            this.fileChannelDocIndex=(FileChannel) Files
                    .newByteChannel(path_docI, EnumSet.of(
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE
                    ));
        }
    }

    /**
     * Aggiorniamo la struttura in memoria
     * @param docTerm
     * @param docId
     */
    public void invert(ArrayList<String> docTerm, Integer docId, String docNo) throws IOException {

        int freq;

        totalLengthDoc+=docTerm.size();

        DocumentIndexElem doc= new DocumentIndexElem(docId, docNo, docTerm.size());
        writeOneDoc(doc);

        for (String t: docTerm) {
            freq = Collections.frequency(docTerm, t);
            //taglio il token a 20 caratteri
            if (t.length()>20)
                t=t.substring(0,20);

            if (positionTerm.get(t) == null)
                //il termine non è presente
                AddTerm(t,docId, freq);
            else
                //il termine è già presente quindi devo solo aggiungere un posting
                AddPost(positionTerm.get(t), docId, freq);
        }
    }

    /**
     * Aggiungo termine nuovo all'array di posting list e all'array del dizionario
     * @param t
     * @param docId
     * @param freq
     */
    private void AddTerm(String t, long docId, int freq) {

        PostingList newTerm = new PostingList(t, docId, freq);
        DictionaryElem d = new DictionaryElem(t, 1, freq);

        this.listTerm.add(newTerm);
        this.listTermDict.add(d);
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
