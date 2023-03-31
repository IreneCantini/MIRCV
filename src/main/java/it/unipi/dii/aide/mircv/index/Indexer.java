package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.data_structures.*;
import it.unipi.dii.aide.mircv.common.file_management.FileUtils;
import it.unipi.dii.aide.mircv.index.utils.IndexUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;

public class Indexer {
    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();
        Flags.setCompression_flag(true);
        Flags.setFilter_flag(true);
        Flags.setMaxScore_flag(false);
        Flags.setDebug_flag(true);
        SPIMI.executeSPIMI("src/main/resources/collection_prova.tsv"); //false: unfiltered, true: filtered
        System.out.println("COSTRUZIONE INVERTED INDEX COMPLETATA");
        long end = System.currentTimeMillis() - start;
        long time = (end/1000)/60;
        System.out.println("Inverted Index built in: " + time + " minutes");




        //Flags.printFlag();

        //IndexUtils.printInvertedIndex(true);

        //IndexUtils.printSkipping();

        //IndexUtils.printDocumentIndex();

       /* RandomAccessFile file = new RandomAccessFile(PATH_TO_VOCABULARY, "rw");
        DictionaryElem d_elem = new DictionaryElem("ciao", 3, 4);
        d_elem.setOffset_docids(32);
        d_elem.setDocids_len(16);
        d_elem.setOffset_tf(16);
        d_elem.setTf_len(8);
        d_elem.setMaxTf(3);

        d_elem.writeDictionaryElemToDisk(file.getChannel());

        DictionaryElem result = new DictionaryElem();

        result.readDictionaryElemFromDisk(0, file.getChannel());
        System.out.println(result.getTerm());
        result.printVocabularyEntry(); */

      /*  RandomAccessFile file_docids = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "rw");
        RandomAccessFile file_freqs = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "rw");

        DictionaryElem d_elem = new DictionaryElem("ciao",4,9);
        d_elem.setOffset_docids(0);
        d_elem.setDocids_len(32);
        d_elem.setOffset_tf(0);
        d_elem.setTf_len(16);
        d_elem.setMaxTf(3);

        ArrayList<Posting> pl_array = new ArrayList<>();
        pl_array.add(new Posting(5,3));
        pl_array.add(new Posting(4,2));
        pl_array.add(new Posting(10,3));
        pl_array.add(new Posting(2,1));
        PostingList pl = new PostingList("ciao", pl_array);

        pl.writeCompressedPostingListToDisk(d_elem, file_docids.getChannel(), file_freqs.getChannel());

        PostingList result = new PostingList("ciao");
        result.readCompressedPostingListFromDisk(d_elem, file_docids.getChannel(), file_freqs.getChannel());

        result.printPostingList(); */

       /* RandomAccessFile file = new RandomAccessFile(PATH_TO_DOCUMENT_INDEX, "rw");

        DocumentIndexElem d = new DocumentIndexElem(2, "1", 15);
        d.writeDocIndexElemToDisk(file.getChannel());
        DocumentIndexElem doc_elem = new DocumentIndexElem();
        doc_elem.readDocumentIndexElemFromDisk(0, file.getChannel());
        System.out.printf("Docid: %d, DocNo: %s, Lenght: %d\n",doc_elem.getDocId(), doc_elem.getDocNo().trim(), doc_elem.getLength()); */
    }
}
