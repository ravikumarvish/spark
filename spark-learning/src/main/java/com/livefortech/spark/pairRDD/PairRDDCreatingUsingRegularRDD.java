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
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author ravikumarvish
 *
 */
public class PairRDDCreatingUsingRegularRDD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Pair RDD Example").setMaster("local[1]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		List<String> a = Arrays.asList("1 John", "2 Peter");
		
        JavaRDD<String> aRDD = context.parallelize(a);
        
        JavaPairRDD<Integer,String> pairRDDS = aRDD.mapToPair(new PairFunction<String, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(String line) throws Exception {
				String words[] = line.split(" ");
				return new Tuple2(Integer.valueOf(words[0]), words[1]);
			}
		});
        
        pairRDDS.saveAsTextFile("out/pairRDDfromRegularRDD");
		
        
		
		
	}

}
