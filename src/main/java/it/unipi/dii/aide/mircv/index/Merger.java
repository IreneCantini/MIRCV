package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.common.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.common.data_structures.PostingList;
import it.unipi.dii.aide.mircv.common.data_structures.min_heap.Comparator;
import it.unipi.dii.aide.mircv.common.data_structures.min_heap.OrderedList;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.*;

public class Merger {
    private static final long[] currentOffsetDictionary = new long[SPIMI.block_number+1];

    public static void executeMerge(Boolean flag_compression) throws IOException, InterruptedException {
        DictionaryElem current_dict_elem = new DictionaryElem();

        DictionaryElem previous_dict_elem = new DictionaryElem();

        PostingList current_pl = new PostingList();
        PostingList previous_pl = new PostingList();

        MappedByteBuffer mappedByteBuffer;

        OrderedList ol;

        boolean firstIteration = true;

        PriorityQueue<OrderedList> pQueue = new PriorityQueue<>(SPIMI.block_number == 0 ? 1 : SPIMI.block_number, new Comparator());

        //create the final files and associate the file Channels to them
        createFinalFile();

        //for-loop for inserting in the priority queue all the first term which are in temporary dictionary files
        for(int i = 0; i <= SPIMI.block_number; i++){
            //read from the temporary dictionary file ith the first term and insert it in the priority queue
            try {
                mappedByteBuffer = RandomAccessFile_map.get(i).get(0).getChannel().map(FileChannel.MapMode.READ_ONLY, 0, 20);

                if (mappedByteBuffer != null) {
                    String[] term = StandardCharsets.UTF_8.decode(mappedByteBuffer).toString().split("\0");
                    ol = new OrderedList(term[0], i);
                    pQueue.add(ol);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        //until the priority queue becomes empty pick the lowest term that hasn't been processed yet
        while (!pQueue.isEmpty()){
            ol = pQueue.poll();

            //insert in the priority queue the next term from the same temporary dictionary file of the term just picked
            if(RandomAccessFile_map.get(ol.getIndex()).get(0).getChannel().size() > currentOffsetDictionary[ol.getIndex()] + 56) {
                mappedByteBuffer = RandomAccessFile_map.get(ol.getIndex()).get(0).getChannel().map(FileChannel.MapMode.READ_ONLY,currentOffsetDictionary[ol.getIndex()], 20);

                if(mappedByteBuffer != null){
                    String[] term = StandardCharsets.UTF_8.decode(mappedByteBuffer).toString().split("\0");
                    ol = new OrderedList(term[0], ol.getIndex());
                    pQueue.add(ol);
                }
            }

            //read from the disk the Dictionary elem corresponding to the term picked from the dictionary file corresponding to the picked index
            current_dict_elem.readDictionaryElemFromDisk(currentOffsetDictionary[ol.getIndex()], RandomAccessFile_map.get(ol.getIndex()).get(0).getChannel());
            currentOffsetDictionary[ol.getIndex()] += 56;

            //read from disk the Posting list corresponding to the current Dictionary
            current_pl.readPostingListFromDisk(current_dict_elem, RandomAccessFile_map.get(ol.getIndex()).get(1).getChannel(), RandomAccessFile_map.get(ol.getIndex()).get(2).getChannel());

            if(firstIteration) {
                //initialize previous dictionary elem
                previous_dict_elem = current_dict_elem;
                previous_pl.getPl().addAll(current_pl.getPl());
                firstIteration = false;
            }else if(current_dict_elem.getTerm().equals(previous_dict_elem.getTerm())) {
                //update previous dictionary elem
                previous_dict_elem.incDf(current_dict_elem.getDf());
                previous_dict_elem.incCf(current_dict_elem.getCf());

                previous_dict_elem.incDocidLen(current_dict_elem.getDocids_len());
                previous_dict_elem.incFreqLen(current_dict_elem.getTf_len());

                previous_pl.getPl().addAll(current_pl.getPl());
            }else{
                //the term picked is different from the previous one, so it is possible to write the dictionary and the posting list to disk
                //if the flag compression is true it is necessary to do the compression before writing it on the disk
                if(!flag_compression){
                    //write posting list to final files
                    previous_pl.writePostingListToDisk(RandomAccessFile_map.get(SPIMI.block_number+1).get(1).getChannel(),RandomAccessFile_map.get(SPIMI.block_number+1).get(2).getChannel());
                }else{
                    //write the compressed posting list
                    previous_pl.writeCompressedPostingListToDisk(previous_dict_elem, RandomAccessFile_map.get(SPIMI.block_number+1).get(1).getChannel(),RandomAccessFile_map.get(SPIMI.block_number+1).get(2).getChannel());
                }

                //update the maxTf field
                previous_dict_elem.updateMaxTf(previous_pl);

                //write dictionary to final file
                previous_dict_elem.writeDictionaryElemToDisk(RandomAccessFile_map.get(SPIMI.block_number+1).get(0).getChannel());

                //update the offset of the final posting list files
                current_dict_elem.setOffset_docids(RandomAccessFile_map.get(SPIMI.block_number+1).get(1).getChannel().size());
                current_dict_elem.setOffset_tf(RandomAccessFile_map.get(SPIMI.block_number +1).get(2).getChannel().size());

                previous_dict_elem = current_dict_elem;
                previous_pl.getPl().clear();
                previous_pl.getPl().addAll(current_pl.getPl());

            }

            //clean the variable for the next iteration
            current_dict_elem = new DictionaryElem();
            current_pl.getPl().clear();
        }

        //clean Memory
        RandomAccessFile_map.clear();

        //delete temporary files
        deleteTemporaryFile();
    }
}