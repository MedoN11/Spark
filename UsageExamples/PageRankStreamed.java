import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.avro.ipc.specific.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.spark_project.guava.base.Function;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;

import scala.Tuple2;
public class PageRank
{
    
    
    private static class Sum implements Function2<Double, Double, Double>
    {
        
        public Double call(Double a, Double b)
        {
            return a + b;
        }
    }
    
    static StringBuilder sb = new StringBuilder();
    
    static class QueryParser
    {
        
        String fileName;
        int ind;
        int cap;
        TreeSet<Integer> nodes;
        ArrayList<Edge> edges;
        ArrayList<Query> queries;
        
        public QueryParser(String fileName) throws IOException
        {
            this.fileName = fileName;
            edges = new ArrayList();
            nodes = new TreeSet();
            // process Edges
            BufferedReader br = new BufferedReader(new FileReader("links.txt"));
            String line;
            int u;
            while((line = br.readLine()) != null)
            {
                StringTokenizer st = new StringTokenizer(line);
                nodes.add(u = new Integer(st.nextToken()));
                while(st.hasMoreTokens())
                {
                    edges.add(new Edge(u,new Integer(st.nextToken())));
                }
            }
            queries = new ArrayList();
            ind = 0;
            divide(7);
            generateFiles();
        }
        
        public void generateFiles() throws IOException
        {
            int ind = 0;
            
            while(ind < queries.size())
            {
                PrintWriter out = new PrintWriter("mm1"+ind+".txt");
                for(Edge e : queries.get(ind).edges)
                {
                    
                    out.write(e.from +" "+e.to+"\n");
                }
                
                out.flush();
                out.close();
                ind++;
            }
        }
        public TreeSet<Integer> getNodes()
        {
            return nodes;
        }
        
        
        public void divide()
        {
            int size = edges.size();
            int k = (int)((double)Math.random() * size) + 1;
            divide(k);
        }
        
        public void divide(int k)
        {
            int size = edges.size();
            System.out.println("aho "+size);
            cap = size / k + Math.min(size % k, 1);
            int ptr = 0;
            while(ptr < size)
            {
                int start = ptr;
                ArrayList<Edge> tmp = new ArrayList();
                while(ptr < size && ptr < start + cap)
                {
                    tmp.add(edges.get(ptr++));
                };
                queries.add(new Query(tmp));
            }
        }
        
    }
    
    
    static class Edge
    {
        int from,to;
        
        public Edge(int from,int to)
        {
            this.from = from;
            this.to = to;
        }
    }
    
    static class Query
    {
        ArrayList<Edge> edges;
        
        public Query(ArrayList<Edge> edges)
        {
            this.edges = edges;
        }
    }
    
    
    public static JavaPairRDD<String,Double> executePageRank(JavaPairRDD<String,Iterable<String>> edges, JavaPairRDD<String,Double> nodeRanks)
    {
        
        System.out.println("Entered page rank");
        for(int i = 0 ; i < 100 ; ++i)
        {
            // what this does is join links by ranks so we end up on key
            // so we end up with something like (1,({1,2,3},2.0))
            // first entry is key ( node ), rest are it's neighbours, and last is rank
            JavaPairRDD<String, Double> contrb = edges.join(nodeRanks).values()
            .flatMapToPair(s ->
                           {
                               int adjSize = Iterables.size(s._1());
                               List<Tuple2<String, Double>> results = new ArrayList<>();
                               for (String n : s._1) {
                                   results.add(new Tuple2<>(n, s._2() / (1.0*adjSize)));
                               }
                               return results.iterator();
                           });
            // now for each node in contribution sum it and multiply/add page rank's chosen values to it
            nodeRanks = contrb.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
            
        }
        List<Tuple2<String, Double>> output = nodeRanks.collect();
        for (Tuple2<?,?> tuple : output) {
            sb.append(tuple._1() + " has rank " + tuple._2() + "."+"\n");
        }
        return nodeRanks;
        
    }
    public static void main(String[]args) throws Throwable
    {
        // assumes Graph is given as edges describied as follows
        // node  neighbour_node
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PageRank");
        JavaSparkContext sc = new JavaSparkContext(conf);
        QueryParser parser = new QueryParser("links.txt");
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>()
        {
            public Tuple2<String,String> call(String x)
            {
                StringTokenizer st = new StringTokenizer(x);
                return new Tuple2(st.nextToken(), st.nextToken());
            }
        };
        Function2<Double, Double, Double> merge = new Function2<Double, Double,Double >()
        { 
            public Double call(Double x,Double y) 
            {
                return Math.max(x, y);
            }
        };
        
        
        int size = parser.queries.size();
        System.out.println(size);
        JavaPairRDD<String, Iterable<String>>edges = null;
        JavaPairRDD<String, Double> nodeRanks = null;
        for(int i = 0 ; i < size ; ++i)
        {
            
            
            
            // streamed query
            sb.append("\n Streamed Query # "+ i );
            JavaRDD<String> lines = sc.textFile("mm1"+i+".txt");
            // set up function for mapping
            
            JavaPairRDD<String, Iterable<String>> newEdges = lines.mapToPair(keyData).distinct().groupByKey();
            if(edges == null)
                edges = newEdges;
            else
            {
                // we need to union both RDD's
                edges = edges.union(newEdges);
            }
            JavaPairRDD<String, Double> newNodeRanks = edges.mapValues(page -> 1.0);
            
            if(nodeRanks == null)
            {
                nodeRanks = newNodeRanks;
            }
            else
            {
                nodeRanks = nodeRanks.union(newNodeRanks);
                nodeRanks = nodeRanks.reduceByKey(merge);
                
            }
            nodeRanks = executePageRank(edges,nodeRanks);
            // generate any delay we want here
            
        }
        System.out.println(sb);
        
        
        
        
        
    }
}
