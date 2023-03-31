package it.unipi.dii.aide.mircv.common.data_structures;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.CollectionInfo_raf;
import static it.unipi.dii.aide.mircv.common.file_management.FileUtils.PATH_TO_COLLECTION_INFO_FILE;

public class CollectionInfo {
    private static long docid_counter = 0;

    private static long total_doc_len = 0;

    public static void setDocid_counter(long docid_counter) {
        CollectionInfo.docid_counter = docid_counter;
    }

    public static void setTotal_doc_len(long total_doc_len) {
        CollectionInfo.total_doc_len = total_doc_len;
    }

    public static long getDocid_counter() {
        return docid_counter;
    }

    public static long getTotal_doc_len() {
        return total_doc_len;
    }

    public static void writeCollectionInfoToDisk() throws IOException {
        ByteBuffer docIndexBuffer = ByteBuffer.allocate(16);
        CollectionInfo_raf.getChannel().position(CollectionInfo_raf.getChannel().size());

        //write the Collection info into file
        docIndexBuffer.putLong(docid_counter);
        docIndexBuffer.putLong(total_doc_len);

        docIndexBuffer = ByteBuffer.wrap(docIndexBuffer.array());

        while(docIndexBuffer.hasRemaining()) {
            CollectionInfo_raf.getChannel().write(docIndexBuffer);
        }
    }

    public static void writeCollectionInfoDebugModeToDisk() throws IOException {

        String collection_info_string = "Totale docid: " + docid_counter + ", Collection length: " + total_doc_len;

        BufferedWriter disk_writer = new BufferedWriter(new FileWriter("src/main/resources/Debug/collection_info_debug.txt", true));
        disk_writer.write(collection_info_string);
        disk_writer.close();
    }

    public static void readCollectionInfoToDisk() throws IOException {
        CollectionInfo_raf = new RandomAccessFile(PATH_TO_COLLECTION_INFO_FILE, "r");

        ByteBuffer docIndexBuffer = ByteBuffer.allocate(16);

        CollectionInfo_raf.getChannel().position(0);

        while (docIndexBuffer.hasRemaining()){
            CollectionInfo_raf.getChannel().read(docIndexBuffer);
        }

        docIndexBuffer.rewind();
        docid_counter = docIndexBuffer.getLong();
        total_doc_len = docIndexBuffer.getLong();
    }

    public static void printCollectionInfo() throws IOException {
        readCollectionInfoToDisk();

        System.out.println("Numero totale di docid: " + docid_counter + ", Lunghezza Collection: " + total_doc_len);

    }
}
