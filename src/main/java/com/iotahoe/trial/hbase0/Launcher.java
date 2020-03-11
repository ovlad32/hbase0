package com.iotahoe.trial.hbase0;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 1. create HBase table 2. if batchsize =0 then insert row by row else insert
 * in batches
 * 
 * TODO: take csv path and batchsize as arguments
 */
public class Launcher {

  private String table1 = "Table1";
  private String family1 = "Family1";
  long t1;
  long t2;
  float total ;
  int count = 0;
 
  String csvFile = "./hbase_data";
  String rowoutFile = "./row_result.txt";
  String batchoutFile = "./batch_result.txt";

  BufferedReader br = null;
  BufferedReader br1 = null;
  String line = "";
  String cvsSplitBy = ",";

  public static void main(String[] args) throws Throwable {
    int batchSize = 0;
    float bufferMb = 0;
    if (args.length>0)  batchSize = Integer.parseInt(args[0]);
    if (args.length>1)  bufferMb = Float.parseFloat(args[1]);
    Launcher l = new Launcher();
    l.go(batchSize, bufferMb);
  }

  private void go(int batchSize, float bufferMb) throws Throwable {
    Configuration hBaseConf = HBaseConfiguration.create();
    hBaseConf.setInt("timeout", 120);
    if (bufferMb != 0) {
      int param = (int) (bufferMb * 1024 * 1024);
      hBaseConf.setInt("hbase.client.write.buffer", param);
      System.err.println("write buffer configured to (bytes):" + param);
    }
    Connection connection = ConnectionFactory.createConnection(hBaseConf);
    System.err.println("Connection configured");
    Admin admin = connection.getAdmin();
    System.err.println("Admin gotten");

    // create HBase table
    try {
      TableName tn = TableName.valueOf(table1.getBytes());
      admin.disableTable(tn);
      System.err.println("Table diabled");
      admin.deleteTable(tn);
      System.err.println("Table dropped");
    } catch( Throwable ignored) {}
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table1.getBytes()));
    desc.addFamily(new HColumnDescriptor(family1));
    admin.createTable(desc);
    System.err.println("Table created");
    Table table = connection.getTable(TableName.valueOf(table1.getBytes()));
    

    // *********************************if batchsize=0 i.e. insert row by row
    if (batchSize == 0) {
      System.err.println("no batches");
      br = new BufferedReader(new FileReader(csvFile));
      while ((line = br.readLine()) != null ) {

        // use comma as separator
        String[] col = line.split(cvsSplitBy);

        byte[] row1 = Bytes.toBytes(col[0]);
        Put p = new Put(row1);
        p.addImmutable(family1.getBytes(), "col1".getBytes(), Bytes.toBytes(col[1]));
        p.addImmutable(family1.getBytes(), "col2".getBytes(), Bytes.toBytes(col[2]));
        p.addImmutable(family1.getBytes(), "col3".getBytes(), Bytes.toBytes(col[3]));
        p.addImmutable(family1.getBytes(), "col4".getBytes(), Bytes.toBytes(col[4]));
        p.addImmutable(family1.getBytes(), "col5".getBytes(), Bytes.toBytes(col[5]));
        p.addImmutable(family1.getBytes(), "col6".getBytes(), Bytes.toBytes(col[6]));
        p.addImmutable(family1.getBytes(), "col7".getBytes(), Bytes.toBytes(col[7]));
        p.addImmutable(family1.getBytes(), "col8".getBytes(), Bytes.toBytes(col[8]));
        p.addImmutable(family1.getBytes(), "col9".getBytes(), Bytes.toBytes(col[9]));
        p.addImmutable(family1.getBytes(), "col10".getBytes(), Bytes.toBytes(col[10]));
        t1 = System.currentTimeMillis();
        table.put(p);
        t2 = System.currentTimeMillis();
        total = total + (t2 - t1);
        System.err.println("Put 1 row in (mls): " + (t2 - t1));

      }
      table.close();
      System.err.println("Table closed");

      PrintStream out = new PrintStream(new FileOutputStream(rowoutFile));
      System.setOut(out);
      System.out.print("Total Time in seconds for row by row= " + total / 1000);

    } else { // *****************************if batchsize is there, insert in batches**********************
      System.err.println("batches of " + batchSize + " rows");
      br1 = new BufferedReader(new FileReader(csvFile));
      List<Put> puts = new ArrayList<>();
      long lineCount = 0;
      while ((line = br1.readLine()) != null) {

        // use comma as separator
        String[] col = line.split(cvsSplitBy);
        //connection.setAutoCommit(false);
        byte[] row1 = Bytes.toBytes(col[0]);
        Put p = new Put(row1);
        p.addImmutable(family1.getBytes(), "col1".getBytes(), Bytes.toBytes(col[1]));
        p.addImmutable(family1.getBytes(), "col2".getBytes(), Bytes.toBytes(col[2]));
        p.addImmutable(family1.getBytes(), "col3".getBytes(), Bytes.toBytes(col[3]));
        p.addImmutable(family1.getBytes(), "col4".getBytes(), Bytes.toBytes(col[4]));
        p.addImmutable(family1.getBytes(), "col5".getBytes(), Bytes.toBytes(col[5]));
        p.addImmutable(family1.getBytes(), "col6".getBytes(), Bytes.toBytes(col[6]));
        p.addImmutable(family1.getBytes(), "col7".getBytes(), Bytes.toBytes(col[7]));
        p.addImmutable(family1.getBytes(), "col8".getBytes(), Bytes.toBytes(col[8]));
        p.addImmutable(family1.getBytes(), "col9".getBytes(), Bytes.toBytes(col[9]));
        p.addImmutable(family1.getBytes(), "col10".getBytes(), Bytes.toBytes(col[10]));
        puts.add(p);
        if (++lineCount >= batchSize)  {
          t1 = System.currentTimeMillis();
          table.put(puts);
          t2 = System.currentTimeMillis();
          total = total + (t2 - t1);
          puts.clear();
          System.err.println("Put "+lineCount+" rows in (mls) " + (t2 - t1));
          lineCount = 0;
        }
      }
      if (!puts.isEmpty())  {
        t1 = System.currentTimeMillis();
        table.put(puts);
        t2 = System.currentTimeMillis();
        System.err.println("Put leftover of " + puts.size() + " rows");
      }
      table.close();
      System.err.println("Table closed");

      PrintStream out1 = new PrintStream(new FileOutputStream(batchoutFile));
      System.setOut(out1);
      System.out.print("Total Time in seconds for batch= " + total / 1000);
    }
  }

}
