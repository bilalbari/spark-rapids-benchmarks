/*
 * SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nvidia.nds_h;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import java.net.*;
import java.math.*;
import java.security.*;


public class GenTable extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new GenTable(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        CommandLineParser parser = new BasicParser();
        getConf().setInt("io.sort.mb", 4);
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("s","scale", true, "scale");
        options.addOption("d","dir", true, "dir");
        options.addOption("t","table", true, "table");
        options.addOption("p", "parallel", true, "parallel");
        options.addOption("o", "overwrite", false, "overwrite existing data");
        options.addOption("r", "range", true, "child range in one data generation run");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!line.hasOption("scale")) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("GenTable", options);
          return 1;
        }
        Path out = new Path(line.getOptionValue("dir"));
        
        int scale = Integer.parseInt(line.getOptionValue("scale"));
        
        String table = "all";
        if(line.hasOption("table")) {
          table = line.getOptionValue("table");
        }
        
        int parallel = scale;

        if(line.hasOption("parallel")) {
          parallel = Integer.parseInt(line.getOptionValue("parallel"));
        }

        int rangeStart = 1;
        int rangeEnd = parallel;

        if(line.hasOption("range")) {
            String[] range = line.getOptionValue("range").split(",");
            if (range.length == 1) {
                System.err.println("Please provide range with comma for both range start and range end.");
                return 1;
            }
            rangeStart = Integer.parseInt(range[0]);
            rangeEnd = Integer.parseInt(range[1]);
            if (rangeStart < 1 || rangeStart > rangeEnd || rangeEnd > parallel) {
                System.err.println("Please provide correct child range: 1 <= rangeStart <= rangeEnd <= parallel");
                return 1;
            }
        }

        if(parallel == 1 || scale == 1) {
          System.err.println("The MR task does not work for scale=1 or parallel=1");
          return 1;
        }

        Path in = genInput(table, scale, parallel, rangeStart, rangeEnd);

        Path dbgen = copyJar(new File("target/dbgen.jar"));
        URI dsuri = dbgen.toUri();
        URI link = new URI(dsuri.getScheme(),
                    dsuri.getUserInfo(), dsuri.getHost(), 
                    dsuri.getPort(),dsuri.getPath(), 
                    dsuri.getQuery(),"dbgen");
        Configuration conf = getConf();
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapreduce.task.timeout",0);
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        DistributedCache.addCacheArchive(link, conf);
        DistributedCache.createSymlink(conf);
        Job job = new Job(conf, "GenTable+"+table+"_"+scale);
        job.setJarByClass(getClass());
        job.setNumReduceTasks(0);
        job.setMapperClass(Dbgen.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 1);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);


        FileSystem fs = FileSystem.get(getConf());
        // delete existing files if "overwrite" is set
        if(line.hasOption("overwrite")) {
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
        }

        // use multiple output to only write the named files
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", 
          TextOutputFormat.class, LongWritable.class, Text.class);

        boolean success = job.waitForCompletion(true);

        // cleanup
        fs.delete(in, false);
        fs.delete(dbgen, false);

        return 0;
    }

    public Path copyJar(File jar) throws Exception {
      MessageDigest md = MessageDigest.getInstance("MD5");
      InputStream is = new FileInputStream(jar);
      try {
        is = new DigestInputStream(is, md);
        // read stream to EOF as normal...
      }
      finally {
        is.close();
      }
      BigInteger md5 = new BigInteger(md.digest()); 
      String md5hex = md5.toString(16);
      Path dst = new Path(String.format("/tmp/%s.jar",md5hex));
      Path src = new Path(jar.toURI());
      FileSystem fs = FileSystem.get(getConf());
      fs.copyFromLocalFile(false, /*overwrite*/true, src, dst);
      return dst; 
    }

    public Path genInput(String table, int scale, int parallel, int rangeStart, int rangeEnd) throws Exception {        long epoch = System.currentTimeMillis()/1000;

        Path in = new Path("/tmp/"+table+"_"+scale+"-"+epoch);
        FileSystem fs = FileSystem.get(getConf());
        FSDataOutputStream out = fs.create(in);

        String[ ] tables = {"c","O","L","P","S","s"};

        for(int i = rangeStart; i <= rangeEnd; i++) {
          String baseCmd = String.format("./dbgen -s %d -C %d -S %d ",scale,parallel,i);
          if(table.equals("all")) {
            for(String t: tables){
              String cmd = baseCmd + String.format("-T %s",t);
              out.writeBytes(cmd+"\n");
            }
          }
          else{
            if(table.equalsIgnoreCase("customers")){
              String cmd = baseCmd + "-T c";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("nation")){
              String cmd = baseCmd + "-T n";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("region")){
              String cmd = baseCmd + "-T r";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("lineItem")){
              String cmd = baseCmd + "-T L";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("orders")){
              String cmd = baseCmd + "-T O";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("parts")){
              String cmd = baseCmd + "-T P";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("patsupp")){
              String cmd = baseCmd + "-T S";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("suppliers")){
              String cmd = baseCmd + "-T s";
              out.writeBytes(cmd + "\n");
            }
          }
        }
        if(table.equals("all")){
          String cmdL = String.format("./dbgen -s %d -T l",scale);
          out.writeBytes(cmdL + "\n");
        }
        out.close();
        return in;
    }

    static String readToString(InputStream in) throws IOException {
      InputStreamReader is = new InputStreamReader(in);
      StringBuilder sb=new StringBuilder();
      BufferedReader br = new BufferedReader(is);
      String read = br.readLine();

      while(read != null) {
        //System.out.println(read);
        sb.append(read);
        read =br.readLine();
      }
      return sb.toString();
    }

    static final class Dbgen extends Mapper<LongWritable,Text, Text, Text> {
      private MultipleOutputs mos;
      protected void setup(Context context) throws IOException {
        mos = new MultipleOutputs(context);
      }
      protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
      }
      protected void map(LongWritable offset, Text command, Mapper.Context context) 
        throws IOException, InterruptedException {
        String parallel="1";
        String child="1";
        String table="";
        String suffix = "";
        String[] cmd = command.toString().split(" ");

        for(int i=0; i<cmd.length; i++) {
          if(cmd[i].equals("-C")) {
            parallel = cmd[i+1];
          }
          if(cmd[i].equals("-S")) {
            child = cmd[i+1];
          }
          if(cmd[i].equals("-T")) {
            table = cmd[i+1];
          }
        }

        System.out.println("Executing command: "+ String.join(" ", cmd));
        Process p = Runtime.getRuntime().exec(cmd, null, new File("dbgen/dbgen/"));
        int status = p.waitFor();
        if(status != 0) {
          String err = readToString(p.getErrorStream());
          throw new InterruptedException("Process failed with status code " + status + "\n" + err);
        }

        File cwd = new File("./dbgen/dbgen");
        if(table.equals("l") || table.equals("n") || table.equals("r"))
          suffix = ".tbl";
        else if(table.equals("c"))
          suffix = String.format("customer.tbl.%s", child);
        else if(table.equals("L"))
          suffix = String.format("lineitem.tbl.%s",child);
        else if(table.equals("O"))
          suffix = String.format("orders.tbl.%s",child);
        else if(table.equals("P"))
          suffix = String.format("part.tbl.%s",child);
        else if(table.equals("S"))
          suffix = String.format("partsupp.tbl.%s",child);
        else if(table.equals("s"))
          suffix = String.format("supplier.tbl.%s",child);

        final String suffixNew = suffix;

        FilenameFilter tables = new FilenameFilter() {
          public boolean accept(File dir, String name) {
            return name.endsWith(suffixNew);
          }
        };

        for(File f: cwd.listFiles(tables)) {
          if(f != null)
          {
            System.out.println("Processing file: "+f.getName());
          }
          final String baseOutputPath = f.getName().replace(suffix.substring(suffix.indexOf('.')), String.format("/data_%s_%s", child, parallel));
          BufferedReader br = new BufferedReader(new FileReader(f));
          String line;
          while ((line = br.readLine()) != null) {
            // process the line.
            mos.write("text", line, null, baseOutputPath);
          }
          br.close();
          f.deleteOnExit();
        }
        System.out.println("Processing complete");
      }
    }
}
