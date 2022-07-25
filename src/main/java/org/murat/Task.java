package org.murat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Path;
import java.sql.*;
import java.text.SimpleDateFormat;


public class Task extends Stream{
    Path file;
    int tskId;
    Connection con = new OracleDBConnection().connect();

    public Task(Path file) {
        this.file=file;
        this.tskId=create(this.file.getFileName().toString());
        parseFile();
        endTask();
    }


    void parseFile() {
        String sql = "INSERT INTO CALL_DETAIL_RECORDS(cdr_id, cdr_dttm, cdr_a_number, cdr_b_number, cdr_country) VALUES (?,?,?,?,?)";
        int recordId = 0;
        long nextCdrId =0;
        try{
            PreparedStatement pstmt = con.prepareStatement(sql);
            Statement stmt = con.createStatement();
            Reader in = new FileReader(file.toFile());
            CSVFormat.Builder formatBuilder = CSVFormat.Builder.create();
            CSVFormat format=formatBuilder.setDelimiter("|")
                    .setTrim(true)
                    .build();
            CSVParser parser = CSVParser.parse(in, format);



            final int batchSize = 1000;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
            for (CSVRecord csvRecord : parser) {
                ResultSet rs = stmt.executeQuery("select call_detail_records_seq.nextval from dual");
                if(rs.next())
                    nextCdrId = rs.getInt(1);
                pstmt.setLong(1,nextCdrId);
                pstmt.setDate(2,new Date(formatter.parse(csvRecord.get(0)).getTime()));
                pstmt.setLong(3,Long.parseLong(csvRecord.get(1)));
                pstmt.setLong(4,Long.parseLong(csvRecord.get(2)));
                int code = new getLongestCode().execute(csvRecord.get(3), itc.getCodeArray() );
                pstmt.setString(5,itc.cache.get(code));
                pstmt.addBatch();
                if(++recordId % batchSize == 0) {
                    pstmt.executeBatch();
                    System.out.println("Loaded " +recordId+ "th record");
                }
            }
            pstmt.executeBatch();
            System.out.println("Loaded " +recordId+ "th record");
            pstmt.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(file.toFile().delete())
                    System.out.println(file.toFile().getName() + " deleted.");
                else System.out.println("ERROR on deletion " +file.toString() );
            } catch (Exception e) {
                System.out.println("ERROR on deletion " +file.toString() );
                e.printStackTrace();
            }
        }
    }




    void endTask() {
        String query = "update task set tsk_end=? where tsk_id="+ tskId;
        try {
            PreparedStatement pstmt = con.prepareStatement(query);
            pstmt.setDate(1, new Date(System.currentTimeMillis()));
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    int create(String filename) {
        int nextTskId = 0;
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select TASK_SEQ.nextval from dual");
            if(rs.next())
                nextTskId = rs.getInt(1);
            String query = "INSERT INTO Task(tsk_id, tsk_file, tsk_start) VALUES (?, ?, ?)";
            PreparedStatement pstmt = con.prepareStatement(query);
            pstmt.setInt(1, nextTskId);
            pstmt.setString(2,filename);
            pstmt.setDate(3, new Date(System.currentTimeMillis()));
            pstmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return nextTskId;
    }
}
