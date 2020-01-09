/**
 * 
 */
package com.livefortech.spark.pairRDD;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author ravikumarvish
 *
 */
public class PairRDDReducerExample {
	
	public static void main(String[] args) {
		
SparkConf conf = new SparkConf().setAppName("Pair RDD Example").setMaster("local[1]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		List<String> a = Arrays.asList("I am John", "I am peter");
		
        JavaRDD<String> aRDD = context.parallelize(a);
        
        JavaRDD<String> aFlatRDD =  aRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        
        JavaPairRDD<String,Integer> pairRDDS = aFlatRDD.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				
				return new Tuple2(line , 1);
			}
        	
		});
        
        /*JavaPairRDD<String,Integer> outputPairRDDS  = pairRDDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1+v2;
			}
		});*/
        
        JavaPairRDD<String,Integer> outputPairRDDS  = pairRDDS.reduceByKey( (x,y) -> x+y);
        outputPairRDDS.saveAsTextFile("out/outputPairRDDS");
		
	}

}
