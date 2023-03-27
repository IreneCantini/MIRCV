package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.file_management.FileUtils;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.common.data_structures.*;
import it.unipi.dii.aide.mircv.index.utils.IndexUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.collection_length;
import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.doc_raf;
import static it.unipi.dii.aide.mircv.index.utils.IndexUtils.cleanMemory;

public class SPIMI {
    //counter of the blocks
    public static int block_number = 0;

    //Dictionary in memory
    public static HashMap<String, DictionaryElem> Dictionary_instance = new HashMap<>();

    //List for maintaining the list of term sorted
    public static ArrayList<String> termList = new ArrayList<>();

    //Posting list of a term in memory
    public static HashMap<String, PostingList> PostingLists_instance = new HashMap<>();

    //All document information
    public static HashMap<Long, Long> Document_index_map = new HashMap<>();

    //total document length
    public static double totdl=0;

    //average document length variable
    public static double avdl=0;

    public static void executeSPIMI(String path_collection) throws IOException, InterruptedException {
        //docid counter
        int freq;
        long docid = 0;
        String docNo;
        DocumentIndexElem doc_elem;
        ArrayList<String> tokens;

        FileUtils.createDocIndexFile();

        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        //open and read collection
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path_collection), StandardCharsets.UTF_8));
        String line = reader.readLine();

        while (line != null) {
            tokens = TextPreprocesser.executeTextPreprocessing(line);

            docid = docid + 1;
            docNo=tokens.get(0);
            tokens.remove(0);

            doc_elem = new DocumentIndexElem(docid, docNo, tokens.size());
            Document_index_map.put(docid, (long)tokens.size());
            totdl+=tokens.size();
            doc_elem.writeDocIndexElemToDisk(doc_raf.getChannel());

            for (String term : tokens) {
                freq = Collections.frequency(tokens, term);

                //taglio il token a 20 caratteri
                if (term.length() > 20)
                    term = term.substring(0, 20);


                //check if the term is already inside the dictionary
                if (!Dictionary_instance.containsKey(term)) {
                    //add the new term to the list of terms of the Dictionary
                    termList.add(term);


                    //add the new term inside the dictionary in memory
                    Dictionary_instance.put(term, new DictionaryElem(term, 1,freq));

                    //create posting list of the term
                    PostingLists_instance.put(term, new PostingList(term, new Posting(docid, freq)));
                } else {
                    ArrayList<Posting> p = PostingLists_instance.get(term).getPl();

                    // check if the current term statistics for the document we are processing have been computed and if not calculate them
                    if(p.get(p.size()-1).getDocID() != docid) {
                        //term already exist in Dictionary, so it is necessary to update only the corresponding Dictionary Entry
                        Dictionary_instance.get(term).incCf(freq);
                        Dictionary_instance.get(term).incDf();

                        //retrieve the posting list of the term and adding a posting
                        PostingLists_instance.get(term).addPosting(new Posting(docid,freq));
                    }
                }
            }

            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                System.out.printf("UTILIZZO MASSIMO CONSENTITO DELLA MEMORIA RAGGIUNTO: SCRITURA DEL BLOCCO '%d' SU DISCO IN CORSO\n", block_number);

                //write block to disk
                if(!IndexUtils.writeBlockToDisk(termList, Dictionary_instance, PostingLists_instance, block_number)){
                    System.out.printf("ERRORE: scrittura del blocco %d su disco non andata a buon fine\n", block_number);
                    break;
                }else{
                    System.out.printf("Scrittura del blocco '%d' completata\n", block_number);
                    block_number++;
                }

                //clear memory used for the elaboration of previous block
                cleanMemory();

                System.gc();

                while (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    Thread.sleep(100);
                }

                System.out.printf("Pulizia della memoria completata, procedo con il nuovo blocco '%d'\n", block_number);
            }
            line = reader.readLine();
            tokens.clear();
        }

        avdl=totdl/docid;
        collection_length = docid;

        //write the final block in memory
        System.out.println("Procedo con la scrittura su disco del blocco finale in memoria");

        if(!IndexUtils.writeBlockToDisk(termList, Dictionary_instance, PostingLists_instance, block_number)){
            System.out.print("ERRORE: scrittura del blocco finale su disco non andata a buon fine\n");
        }else{
            System.out.printf("Scrittura del blocco finale %d completata\n", block_number);
        }

        //merge phase
        System.out.println("OPERAZIONE DI MERGING IN CORSO");
        long start = System.currentTimeMillis();

        Merger.executeMerge();

        System.out.println("OPERAZIONE DI MERGING COMPLETATA");

        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("Merge operation executed in: " + time + " minutes");
    }
}
