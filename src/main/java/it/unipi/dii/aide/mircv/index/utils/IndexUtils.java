package it.unipi.dii.aide.mircv.index.utils;

import it.unipi.dii.aide.mircv.common.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.common.data_structures.DocumentIndexElem;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;
import static it.unipi.dii.aide.mircv.index.SPIMI.*;

public class IndexUtils {

    public static void cleanMemory(){
        Dictionary_instance.clear();
        PostingLists_instance.clear();
        termList.clear();
    }

    /**
     * Function that write one block to the disk.
     * @param termList is the array containing an array of terms founded in the collection
     * @param partialDictionary contain the partial dictionary created so far
     * @param partialPostingLists contain the partial posting list created so far
     * @param block_number the current number of the block
     * @return True if all was good
     * @throws IOException if the channel is not found
     */
    public static boolean writeBlockToDisk(ArrayList<String> termList, HashMap<String, DictionaryElem> partialDictionary,
                                           HashMap<String, PostingList> partialPostingLists, int block_number) throws IOException {

        /* Create SPIMI output file */
        createTemporaryFile();

        Collections.sort(termList);

        /* Write PostingLists into disk and update the offset of the two posting list in the corresponding Dictionary */
        for (String term: termList){

            /* Retrieve the posting list of the term */
            PostingList tmp_pl = partialPostingLists.get(term);

            /* Retrieve the Dictionary entry of the term */
            DictionaryElem d_elem = partialDictionary.get(term);

            /* Update the offset and length of the corresponding docID posting list */
            d_elem.setOffset_docids(RandomAccessFile_map.get(block_number).get(1).getChannel().size());

            /* Update the offset and length of the corresponding frequencies posting list */
            d_elem.setOffset_tf(RandomAccessFile_map.get(block_number).get(2).getChannel().size());

            /* Update the maxTF field */
            d_elem.updateMaxTf(tmp_pl);

            /* Write the posting list of docID and frequency in the temporary file */
            tmp_pl.writePostingListToDisk(null, d_elem, RandomAccessFile_map.get(block_number).get(1).getChannel(), RandomAccessFile_map.get(block_number).get(2).getChannel());

            /* Write the dictionary element in the corresponding output file */
            partialDictionary.put(term,d_elem);
            d_elem.writeDictionaryElemToDisk(RandomAccessFile_map.get(block_number).get(0).getChannel());
        }

        return true;
    }

    /**
     * Function to print the Document Index elements
     * @throws IOException if the channel is not found
     */
    public static void printDocumentIndex() throws IOException {
        int position = 0;
        DocumentIndexElem doc_elem;

        FileChannel docindexFChannel = new RandomAccessFile(PATH_TO_DOCUMENT_INDEX, "rw").getChannel();

        while (position < Files.size(Path.of(PATH_TO_DOCUMENT_INDEX))){
            doc_elem = new DocumentIndexElem();
            doc_elem.readDocumentIndexElemFromDisk(position, docindexFChannel);
            System.out.printf("Docid: %d, DocNo: %s, Lenght: %d\n",doc_elem.getDocId(), doc_elem.getDocNo().trim(), doc_elem.getLength());
            position += 36;
        }
    }

    /**
     * Function that print the Inverted Index
     * @param flag that specifies the read mode
     * (TRUE: read with compression, FALSE: read without compression )
     * @throws IOException if the channel is not found
     */
    public static void printInvertedIndex(boolean flag) throws IOException {

        int position = 0;
        DictionaryElem d_elem;
        PostingList pl;

        /* Open file channels to output files */
        FileChannel dictionaryFchannel = new RandomAccessFile(PATH_TO_VOCABULARY, "rw").getChannel();
        FileChannel docidsFchannel = new RandomAccessFile(PATH_TO_DOCIDS_POSTINGLIST, "rw").getChannel();
        FileChannel freqsFchannel = new RandomAccessFile(PATH_TO_FREQ_POSTINGLIST, "rw").getChannel();

        while (position < Files.size(Path.of(PATH_TO_VOCABULARY))) {
            d_elem = new DictionaryElem();
            d_elem.readDictionaryElemFromDisk(position, dictionaryFchannel);
            System.out.printf("Term: '%s'\n", d_elem.getTerm());
            d_elem.printVocabularyEntry();

            pl = new PostingList(d_elem.getTerm());

            if (flag)
                pl.readCompressedPostingListFromDisk(d_elem, docidsFchannel, freqsFchannel);
            else
                pl.readPostingListFromDisk(d_elem, docidsFchannel, freqsFchannel);

            pl.printPostingList();

            position += 92;
        }
    }
}
