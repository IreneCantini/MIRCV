package it.unipi.dii.aide.mircv.common.data_structures;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class DocumentIndexElem {
    private String docNo;
    private long docId;
    private long length;

    public DocumentIndexElem() {
        this.docId = 0;
        this.docNo = "";
        this.length = 0;
    }

    public DocumentIndexElem(long docId, String docNo, long length) {

        int diffLength = 20 - docNo.length();

        if (diffLength != 0 )
            /* Let's concatenate " " to get to the fixed size of 20 characters */
            docNo = docNo + " ".repeat(Math.max(0, diffLength));

        this.docId = docId;
        this.docNo = docNo;
        this.length = length;
    }

    public void setDocId(long docId) {
        this.docId = docId;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getDocId() {
        return docId;
    }

    public String getDocNo() {
        return docNo;
    }

    public long getLength() {
        return length;
    }

    /**
     * This function write down a new Document Index element into disk
     * @param docIndexFileChannel FileChannel related to Document Index
     * @throws IOException if the channel is not found
     */
    public void writeDocIndexElemToDisk(FileChannel docIndexFileChannel) throws IOException {

        ByteBuffer docIndexBuffer = ByteBuffer.allocate(36);
        docIndexFileChannel.position(docIndexFileChannel.size());
        CharBuffer charBuffer = CharBuffer.allocate(20);

        for (int i = 0; i < this.docNo.length(); i++)
            charBuffer.put(i, this.docNo.charAt(i));

        /* Write the document element fields into file */
        docIndexBuffer.put(StandardCharsets.UTF_8.encode(charBuffer));
        docIndexBuffer.putLong(this.docId);
        docIndexBuffer.putLong(this.length);

        docIndexBuffer = ByteBuffer.wrap(docIndexBuffer.array());

        while(docIndexBuffer.hasRemaining())
            docIndexFileChannel.write(docIndexBuffer);
    }

    /**
     * This function read a new Document Index element from disk
     * @param start_position offset where to start reading
     * @param docIndexFileChannel FileChannel related to Document Index
     * @throws IOException if the channel is not found
     */
    public void readDocumentIndexElemFromDisk(int start_position, FileChannel docIndexFileChannel) throws IOException {

        ByteBuffer docIndexBuffer = ByteBuffer.allocate(20);
        docIndexFileChannel.position(start_position);

        while (docIndexBuffer.hasRemaining())
            docIndexFileChannel.read(docIndexBuffer);

        this.setDocNo(new String(docIndexBuffer.array(), StandardCharsets.UTF_8).trim());

        docIndexBuffer = ByteBuffer.allocate(16);

        while (docIndexBuffer.hasRemaining())
            docIndexFileChannel.read(docIndexBuffer);

        docIndexBuffer.rewind();
        this.setDocId(docIndexBuffer.getLong());
        this.setLength(docIndexBuffer.getLong());
    }

    /**
     * This function write down a new Document Index element into disk with DEBUG-MODE
     * @throws IOException if the channel is not found
     */
    public void writeDocumentElemDebugModeToDisk() throws IOException {

        String doc_elem_string = " Docno: " + this.docNo.trim() + " docID: " + this.docId + " length: " + this.length;
        BufferedWriter disk_writer = new BufferedWriter(
                new FileWriter("src/main/resources/Debug/document_debug.txt", true));
        disk_writer.write(doc_elem_string);
        disk_writer.close();
    }
}
