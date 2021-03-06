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
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author ravikumarvish
 *
 */
public class PairRDDGroupByKeyExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Pair RDD Example").setMaster("local[1]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		List<String> a = Arrays.asList("UK John", "UK Peter" , "US Sam");
		
        JavaRDD<String> aRDD = context.parallelize(a);
        
        JavaPairRDD<String,String> pairRDDS = aRDD.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				String words[] = line.split(" ");
				return new Tuple2(words[0], words[1]);
			}
        	
		});
        
        JavaPairRDD<String, Iterable<String>> output  = pairRDDS.groupByKey();
        
        /*JavaPairRDD<String,Integer> outputPairRDDS  = pairRDDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1+v2;
			}
		});*/
        
        output.saveAsTextFile("out/outputGroupByKey");
		
	}

}
