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
import java.util.Objects;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;
import static it.unipi.dii.aide.mircv.index.SPIMI.*;

public class IndexUtils {
    public static void cleanMemory(){
        Dictionary_instance.clear();
        PostingLists_instance.clear();
        termList.clear();
    }
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

    public static void printInvertedIndex(boolean flag) throws IOException {
        int position = 0;
        DictionaryElem d_elem;
        PostingList pl;

        //open file channels to output files
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
                //compression mode
                pl.readCompressedPostingListFromDisk(d_elem, docidsFchannel, freqsFchannel);
            else
                pl.readPostingListFromDisk(d_elem, docidsFchannel, freqsFchannel);


            //test for checking if the inverted index is built properly
            /*if(pl.getTerm().equals("amid"))
            {
                System.out.println("dentro if");
                System.out.printf("Term: '%s'\n", d_elem.getTerm());
                d_elem.printVocabularyEntry();
                pl.printPostingList();
            }*/

            pl.printPostingList();

            position += 56;
        }
    }

    public static boolean writeBlockToDisk(ArrayList<String> termList, HashMap<String, DictionaryElem> partialDictionary, HashMap<String, PostingList> partialPostingLists, int block_number) throws IOException {
        //create SPIMI output file
        createTemporaryFile();

        Collections.sort(termList);

        for(String term: termList){
            /* write PostingLists into disk and update the offset of the two posting list in the corresponding Dictionary */

            //retrieve the posting list of the term
            PostingList tmp_pl = partialPostingLists.get(term);

            //retrieve the dictionay entry of the term
            DictionaryElem d_elem = partialDictionary.get(term);

            //update the offset and length of the corresponding docid posting list
            d_elem.setOffset_docids(RandomAccessFile_map.get(block_number).get(1).getChannel().size());
            d_elem.setDocids_len(tmp_pl.getPl().size() * 8);

            //update the offset and length of the corresponding freq posting list
            d_elem.setOffset_tf(RandomAccessFile_map.get(block_number).get(2).getChannel().size());
            d_elem.setTf_len(tmp_pl.getPl().size() * 4);

            //update the maxTf field
            d_elem.updateMaxTf(tmp_pl);

            //write the PostingList of docid and freq in the temporary File
            tmp_pl.writePostingListToDisk(RandomAccessFile_map.get(block_number).get(1).getChannel(), RandomAccessFile_map.get(block_number).get(2).getChannel());

            partialDictionary.put(term,d_elem);

            //write the Dictionary elem in the corresponding output file
            d_elem.writeDictionaryElemToDisk(RandomAccessFile_map.get(block_number).get(0).getChannel());
        }
        return true;
    }
}
