package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.file_management.FileUtils;
import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.common.data_structures.*;
import it.unipi.dii.aide.mircv.index.utils.IndexUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.collection_length;
import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.doc_raf;
import static it.unipi.dii.aide.mircv.index.utils.IndexUtils.cleanMemory;

public class SPIMI {

    /* Counter of blocks */
    public static int block_number = 0;

    /* Dictionary in memory */
    public static HashMap<String, DictionaryElem> Dictionary_instance = new HashMap<>();

    /* List for maintaining the list of term sorted */
    public static ArrayList<String> termList = new ArrayList<>();

    /* Posting list of a term in memory */
    public static HashMap<String, PostingList> PostingLists_instance = new HashMap<>();

    /* Document information */
    public static ArrayList<Integer> DocsLen = new ArrayList<>();

    /* Total document length */
    public static double totdl = 0;

    /* Average document length variable */
    public static double avdl = 0;

    public static long timeTextPreprocessing=0;

    public static void executeSPIMI(String path_collection) throws IOException, InterruptedException {

        int freq;
        long docid = 0;
        String docNo;
        DocumentIndexElem doc_elem;
        ArrayList<String> tokens;

        FileUtils.createDocIndexFile();

        /* Setting the total used memory to 80% */
        long MaxUsableMemory = Runtime.getRuntime().maxMemory() * 80 / 100;

        /* Open and read collection */
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path_collection), StandardCharsets.UTF_8));
        String line = reader.readLine();

        TextPreprocesser.stopwords_global= Files.readAllLines(Paths.get("src/main/resources/stopwords.txt"));

        while (line != null) {
            long start = System.currentTimeMillis();
            tokens = TextPreprocesser.executeTextPreprocessing(line);
            long end = System.currentTimeMillis() - start;
            timeTextPreprocessing+=end;

            docid = docid + 1;
            docNo = tokens.get(0);
            tokens.remove(0);

            doc_elem = new DocumentIndexElem(docid, docNo, tokens.size());
            DocsLen.add(tokens.size());
            totdl += tokens.size();
            doc_elem.writeDocIndexElemToDisk(doc_raf.getChannel());

            if (Flags.isDebug_flag())
                doc_elem.writeDocumentElemDebugModeToDisk();

            for (String term : tokens) {
                freq = Collections.frequency(tokens, term);

                /* Cut the token to 20 characters */
                if (term.length() > 20)
                    term = term.substring(0, 20);

                /* Check if the term is already inside the dictionary */
                if (!Dictionary_instance.containsKey(term)) {

                    /* Add the new term to the list of terms of the Dictionary */
                    termList.add(term);

                    /* Add the new term inside the dictionary in memory */
                    Dictionary_instance.put(term, new DictionaryElem(term, 1,freq));

                    /* create posting list of the term */
                    PostingLists_instance.put(term, new PostingList(term, new Posting(docid, freq)));

                }else {
                    ArrayList<Posting> p = PostingLists_instance.get(term).getPl();

                    /* Check if the current term statistics for the document we are processing have been computed and if not calculate them */
                    if (p.get(p.size()-1).getDocID() != docid) {

                        /* Term already exist in Dictionary, so it is necessary to update only the corresponding Dictionary Entry */
                        Dictionary_instance.get(term).incCf(freq);
                        Dictionary_instance.get(term).incDf();

                        /* Retrieve the posting list of the term and adding a posting */
                        PostingLists_instance.get(term).addPosting(new Posting(docid,freq));
                    }
                }
            }

            if (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                System.out.printf("(INFO) MAXIMUM PERMITTED USE OF MEMORY ACHIEVED: WRITING BLOCK '%d' ON CURRENT DISC.\n", block_number);

                /* Write block to disk */
                if (!IndexUtils.writeBlockToDisk(termList, Dictionary_instance, PostingLists_instance, block_number)){
                    System.out.printf("(ERROR): %d block write to disk failed\n", block_number);
                    break;
                }else {
                    System.out.printf("(INFO) Writing block '%d' completed\n", block_number);
                    block_number++;
                }

                /* Clear memory used for the elaboration of previous block */
                cleanMemory();
                System.gc();

                /* Check if total memory is greater than usable memory */
                while (Runtime.getRuntime().totalMemory() > MaxUsableMemory) {
                    System.out.println();
                    Thread.sleep(100);
                }

                System.out.printf("(INFO) Memory cleaning completed, proceed with the new block '%d'\n", block_number);
            }
            line = reader.readLine();
            tokens.clear();
        }

        avdl = totdl/docid;
        collection_length = docid;

        /* Write the final block in memory */
        System.out.println("(INFO) Proceed with writing the final block to disk in memory");

        if (!IndexUtils.writeBlockToDisk(termList, Dictionary_instance, PostingLists_instance, block_number))
            System.out.print("(ERROR): final block write to disk failed\n");
        else
            System.out.printf("(INFO) Final block write %d completed\n", block_number);

        /* Merge phase */
        System.out.println("\n*** MERGING OPERATION IN PROGRESS ***");
        long start = System.currentTimeMillis();
        Merger.executeMerge();
        System.out.println("*** MERGING OPERATION COMPLETED ***");
        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("\nMerge operation executed in: " + time + " minutes");

        /* Set the final Collection info and write it to disk */
        CollectionInfo.setTotal_doc_len((long)totdl);
        CollectionInfo.setDocid_counter(docid);
        CollectionInfo.writeCollectionInfoToDisk();

        /* Write the Flags to disk */
        Flags.writeFlagToDisk();

        if (Flags.isDebug_flag())
            CollectionInfo.writeCollectionInfoDebugModeToDisk();
    }
}
