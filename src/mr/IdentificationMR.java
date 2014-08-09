package mr;

/**
 *
 * @author (13511601) Whilda Chaq
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
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
                FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), new Configuration());     
                SetupParamObj(hdfs);
                SetupQueryPerson(hdfs);
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
            for(Map.Entry<String,Float> entry : map.entrySet()) {
                JSONObject jobj = new JSONObject();
                jobj.put("key", entry.getKey());
                jobj.put("value", entry.getValue());
                Value.set(jobj.toJSONString());
                context.write(key, Value);
                if(++i == k) break;
            }
        }
        
        
        public void SetupParamObj(FileSystem hdfs) throws URISyntaxException, IOException{
            BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs://localhost:9000/parameter.json"))));
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
            this.sorted_map = new TreeMap<>();
            this.filename = new Text();
            this.matchingScore = new FloatWritable();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                super.setup(context);
                FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), new Configuration());     
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
            for(Map.Entry<String,Float> entry : map.entrySet()) {
                filename.set(entry.getKey());
                matchingScore.set(entry.getValue());
                context.write(filename, matchingScore);
                if(++i == k) break;
            }
        }
        
        public void SetupParamObj(FileSystem hdfs) throws URISyntaxException, IOException{
            BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs://localhost:9000/parameter.json"))));
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
            JSONObject paramJObj = new JSONObject();
            paramJObj.put("K", 20000);
            paramJObj.put("Threshold", 0);
            paramJObj.put("PathInput", "hdfs://localhost:9000/db/zip-Summary/*.zip");
            paramJObj.put("PathOutput", "hdfs://localhost:9000/result");
            paramJObj.put("Query", "hdfs://localhost:9000/query/FVC2000-Db1_a_1_1.template");
            
            Configuration conf = new Configuration();
            
            FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), conf );
            Path file = new Path("hdfs://localhost:9000/parameter.json");
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
            long startTime = System.currentTimeMillis();
            if(job.waitForCompletion(true)){
                System.out.println("DONE!!");
            }else{
                System.out.println("SOMETHING'S WRONG!!");
            }
            long EndTime = System.currentTimeMillis();
            System.out.println("Time : "+ ((float) (EndTime-startTime)) + " ms");
        } catch (IOException | IllegalStateException | IllegalArgumentException | InterruptedException | ClassNotFoundException e) {
            System.out.println("User Log : "+e.getMessage());
        }
    }
}
