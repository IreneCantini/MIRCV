package it.unipi.dii.aide.mircv.cli;

import it.unipi.dii.aide.mircv.common.text_preprocessing.TextPreprocesser;
import it.unipi.dii.aide.mircv.query_processing.QueryPreprocesser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Write Query or 'exit' command to terminate: ");
        Scanner sc=new Scanner(System.in);

        String query;
        ArrayList<String> tokens;
        while(true)
        {
            query = sc.nextLine();

            if(query==null || query.isEmpty()){
                System.out.println("Error: Insert a correct query.");
                continue;
            }

            if(query.equals("exit"))
                break;

            //flag = true to execute stemming
            //flag = false to not execute stemming
            tokens = TextPreprocesser.executeTextPreprocessing(query, true);

            QueryPreprocesser.executeQueryProcesser(tokens);

        }


    }
}