import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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
public class PageRankNoStream
{
    
    
    private static class Sum implements Function2<Double, Double, Double>
    {
        
        public Double call(Double a, Double b)
        {
            return a + b;
        }
    }
    
    
    public static void main(String[]args) throws Throwable
    {
        // assumes Graph is given as edges describied as follows
        // node  neighbour_node
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Page Rank");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // read data in an RDD each line represents an entry in the RDD
        JavaRDD<String> lines = sc.textFile("links.txt");
        // set up function for mapping
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>()
        {
            public Tuple2<String,String> call(String x)
            {
                StringTokenizer st = new StringTokenizer(x);
                return new Tuple2(st.nextToken(), st.nextToken());
            }
        };
        // map each node to it's neighbours
        // looks as follows (node,{neigbbours}) in an iterable string
        JavaPairRDD<String, Iterable<String>> edges = lines.mapToPair(keyData).distinct().groupByKey();
        // intialise rank or each page to 1.0
        // let's say we have an edge(u,v) this maps to (u,1.0).
        JavaPairRDD<String, Double> nodeRanks = edges.mapValues(page -> 1.0);
        
        
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
            System.out.println(tuple._1() + " has rank " + tuple._2() + ".");
        }
        
        
        
    }
}
