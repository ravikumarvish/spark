/**
 * 
 */
package com.livefortech.spark.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * @author ravikumarvish
 *
 */
public class RDDPersistence {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Spark Persistence").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Union
        List<Integer> a = Arrays.asList(1, 2, 3 , 4);
        JavaRDD<Integer> aRDD = sc.parallelize(a);
        
        aRDD.persist(StorageLevel.MEMORY_ONLY());
        
        aRDD.reduce((x,y)->x+y);
        aRDD.count();
        
        
        
        
        
        
        // 

	}

}