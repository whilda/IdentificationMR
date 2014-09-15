package mr;

/**
 *
 * @author (13511601) Whilda Chaq
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import sourceafis.simple.AfisEngine;
import sourceafis.simple.Fingerprint;
import sourceafis.simple.Person;

public class IdentificationMR {
    static class ValueComparator implements Comparator<String> {
        Map<String, Float> base;
        public ValueComparator(Map<String, Float> base) {
            this.base = base;
        }

        @Override
        public int compare(String a, String b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            }
        }
    }
    public static class StaticMapper extends Mapper<Text, BytesWritable, Text, Text>{
        /* Map */
        private final Text key;
        private final Text Value;
        /* Identification */
        private final AfisEngine afisEngine;
        private JSONObject paramJObj;
        private final Fingerprint fpQuery;
        private final Fingerprint fpData;
        private Person queryPerson;
        private Person dataPerson;
        /* Top Ten */
        private final HashMap<String,Float> map;
        private final ValueComparator bvc;
        private final TreeMap<String,Float> sorted_map;

        private int idMapper;
        private String hostName;
        
        public StaticMapper() {
            this.map = new HashMap<>();
            this.bvc = new ValueComparator(map);
            this.sorted_map  = new TreeMap<>(bvc);
            
            this.afisEngine = new AfisEngine();
            this.key = new Text("#");
            this.Value = new Text();
            
            fpQuery = new Fingerprint();
            queryPerson = new Person(fpQuery);
            
            fpData = new Fingerprint();
            dataPerson = new Person(fpData);
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                super.setup(context);
                FileSystem hdfs = FileSystem.get( new URI( "hdfs://master:9000" ), new Configuration());     
                SetupParamObj(hdfs);
                SetupQueryPerson(hdfs);
                
                try {
                    InetAddress addr = InetAddress.getLocalHost();
                    hostName = addr.getHostName();
                } catch (UnknownHostException ex) {
                    Logger.getLogger(IdentificationMR.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (IOException | InterruptedException | URISyntaxException ex) {
                Logger.getLogger(IdentificationMR.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            try{
                String fileNameString = key.toString();
                byte[] fileContent = value.getBytes();
                float ms = GetMatchingScore(fileContent);
                
                try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/hduser/Logs", true)))) 
                {
                    out.println(fileNameString+"\t"+ms+"\t"+hostName);
                }catch (Throwable e) {
                    System.out.println("Error write : "+e.getMessage());
                }
                
                if(Float.parseFloat(paramJObj.get("Threshold").toString()) <= ms){
                    map.put(fileNameString, ms);
                }
            }catch(IOException | URISyntaxException ex){
                Logger.getLogger(IdentificationMR.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            sorted_map.putAll(map);
            int i = 0;
            int k = Integer.parseInt(paramJObj.get("K").toString());
            for(Map.Entry<String,Float> entry : sorted_map.entrySet()) {
                JSONObject jobj = new JSONObject();
                jobj.put("key", entry.getKey());
                jobj.put("value", entry.getValue());
                Value.set(jobj.toJSONString());
                context.write(key, Value);
                if(++i == k) break;
            }
        }
        
        
        public void SetupParamObj(FileSystem hdfs) throws URISyntaxException, IOException{
            BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs://master:9000/parameter.json"))));
            String str = bfr.readLine();
            if(str != null){
                paramJObj = (JSONObject) JSONValue.parse(str);
            }else{
                paramJObj = null;
            }
        }

        private void SetupQueryPerson(FileSystem hdfs) throws IOException {
            fpQuery.setIsoTemplate(GetByteQuery(hdfs));
        }
        private byte[] GetByteQuery(FileSystem hdfs) throws IOException{
            InputStream is = hdfs.open(new Path(paramJObj.get("Query").toString()));
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            int nRead;
            byte[] data = new byte[16384];

            while ((nRead = is.read(data, 0, data.length)) != -1) {
              buffer.write(data, 0, nRead);
            }

            buffer.flush();

            return buffer.toByteArray();
        }
        public float GetMatchingScore(byte[] template) throws URISyntaxException, IOException{
            try{
                fpData.setIsoTemplate(template);
                return afisEngine.verify(queryPerson, dataPerson);
            }catch(Exception ex){
                return -1;
            }
        }
    }    
    
    public static class StaticReducer extends Reducer<Text,Text,Text,FloatWritable> {
        /* Reduce */
        private final Text filename;
        private final FloatWritable matchingScore;
        
        /* Identification */
        private JSONObject paramJObj;
        
        /* Top Ten */
        private final HashMap<String,Float> map;
        private final ValueComparator bvc;
        private final TreeMap<String,Float> sorted_map;

        public StaticReducer() {
            this.map = new HashMap<>();
            this.bvc = new ValueComparator(map);
            this.sorted_map = new TreeMap<>(bvc);
            
            this.filename = new Text();
            this.matchingScore = new FloatWritable();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                super.setup(context);
                FileSystem hdfs = FileSystem.get( new URI( "hdfs://master:9000" ), new Configuration());     
                SetupParamObj(hdfs);
            } catch (IOException | InterruptedException | URISyntaxException ex) {
                Logger.getLogger(IdentificationMR.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values){
                JSONObject jObj = (JSONObject) JSONValue.parse(value.toString());
                map.put(jObj.get("key").toString(), Float.parseFloat(jObj.get("value").toString()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            sorted_map.putAll(map);
            int i = 0;
            int k = Integer.parseInt(paramJObj.get("K").toString());
            for(Map.Entry<String,Float> entry : sorted_map.entrySet()) {
                filename.set(entry.getKey());
                matchingScore.set(entry.getValue());
                context.write(filename, matchingScore);
                if(++i == k) break;
            }
        }
        
        public void SetupParamObj(FileSystem hdfs) throws URISyntaxException, IOException{
            BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs://master:9000/parameter.json"))));
            String str = bfr.readLine();
            if(str != null){
                paramJObj = (JSONObject) JSONValue.parse(str);
            }else{
                paramJObj = null;
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        try {            
            Configuration conf = new Configuration();
            JSONObject paramJObj = new JSONObject();
            
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            String paramK = "";
            String paramThreshold = "";
            String paramPathQuery = "";
            String paramPathInput = "";
            String paramPathOutput = "";
            if (otherArgs.length == 4) {
                paramK = otherArgs[0];
                paramThreshold = otherArgs[1];
                if(otherArgs[2].equals("1")){
                    paramPathQuery = "/Query/FVC2000-Db1_b_105_8.template";
                    paramPathInput = "/Percobaan1";
                }else if(otherArgs[2].equals("2")){
                    paramPathQuery = "/Query/FVC2002-Db4_a_22_4.template";
                    paramPathInput = "/Percobaan2";
                }else if(otherArgs[2].equals("3")){
                    paramPathQuery = "/Query/FVC2004-Db3_a_99_3.template";
                    paramPathInput = "/Percobaan3";
                }else if(otherArgs[2].equals("4")){
                    paramPathQuery = "/Query/FVC2006-Db2_a_84_11.template";
                    paramPathInput = "/Percobaan4";
                }
                paramPathOutput = otherArgs[3];
            }else if (otherArgs.length == 5) {
                paramK = otherArgs[0];
                paramThreshold = otherArgs[1];
                paramPathQuery = otherArgs[2];
                paramPathInput = otherArgs[3];
                paramPathOutput = otherArgs[4];
            }else{
                System.err.println("Usage: IdentificationMR <K> <Th> <Q> <in> <out>");
                System.err.println("K: number for ouput, ex : 5");
                System.err.println("Th: threshold for matching score, ex : 10");
                System.err.println("Q: Path to query template, ex : /dir/a.template");
                System.err.println("in: Path to directory input, ex : /dir");
                System.err.println("out: Path to directory output, ex : /dir");
                System.exit(2);
            }
            
            paramJObj.put("K", paramK);
            paramJObj.put("Threshold", paramThreshold);
            paramJObj.put("Query", "hdfs://master:9000"+paramPathQuery);
            paramJObj.put("PathInput", "hdfs://master:9000"+paramPathInput+"/*.zip");
            paramJObj.put("PathOutput", "hdfs://master:9000"+paramPathOutput);
            
            FileSystem hdfs = FileSystem.get( new URI( "hdfs://master:9000" ), conf );
            Path file = new Path("hdfs://master:9000/parameter.json");
            if ( hdfs.exists( file )) 
            { 
                if(hdfs.delete( file, true )) System.out.println("DELETE JSON");
                else System.out.println("FAILED DELETE JSON");
            } 
            
            OutputStream os = hdfs.create(file);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            br.write(paramJObj.toJSONString());
            br.close();
            hdfs.close();
            
            Job job = new Job(conf, "Identification");
            job.setJarByClass(IdentificationMR.class);

            job.setMapperClass(StaticMapper.class);
            job.setReducerClass(StaticReducer.class);

            job.setInputFormatClass(ZipFileInputFormat.class);
            
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            FileInputFormat.addInputPath(job, new Path(paramJObj.get("PathInput").toString()));
            FileOutputFormat.setOutputPath(job, new Path(paramJObj.get("PathOutput").toString()));
            
            Date startDate = new Date();
            MeasurementThread mr = new MeasurementThread("Identification", Runtime.getRuntime());
            mr.start();
            if(job.waitForCompletion(true)){
                System.out.println("DONE!!");
            }else{
                System.out.println("SOMETHING'S WRONG!!");
            }
            Date endDate = new Date();
            
            DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            System.out.println("Start\t: "+ df.format(startDate));
            System.out.println("Finish\t: "+ df.format(endDate));
            
            long diff = endDate.getTime() - startDate.getTime();
            System.out.println("Time (ms)\t: "+ diff);
            mr.Stop();
            mr.stop();
            mr.PrintAverage();
        } catch (IOException | IllegalStateException | IllegalArgumentException | InterruptedException | ClassNotFoundException e) {
            System.out.println("User Log : "+e.getMessage());
        }
    }
}
