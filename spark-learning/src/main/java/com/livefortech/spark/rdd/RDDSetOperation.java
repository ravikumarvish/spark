/**
 * 
 */
package com.livefortech.spark.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author ravikumarvish
 *
 */
public class RDDSetOperation {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("SetOperation").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Union
        List<Integer> a = Arrays.asList(1, 2, 3 , 4);
        JavaRDD<Integer> aRDD = sc.parallelize(a);
        
        List<Integer> b = Arrays.asList(4, 5, 6);
        JavaRDD<Integer> bRDD = sc.parallelize(b);
        
        JavaRDD<Integer> cRDD = aRDD.union(bRDD);
        List<Integer> cOutput = cRDD.collect();
        System.out.println("Union Output ***********************"+cOutput);
        
        // Intersection
        
        List<Integer> d = Arrays.asList(1, 2, 3 , 2, 4);
        JavaRDD<Integer> dRDD = sc.parallelize(d);
        
        List<Integer> e = Arrays.asList(2, 4, 6);
        JavaRDD<Integer> eRDD = sc.parallelize(e);
        
        JavaRDD<Integer> fRDD = dRDD.intersection(eRDD);
        List<Integer> fOutput = fRDD.collect();
        System.out.println("Intersection Output*****************"+fOutput);
        
        // SUBTRACT
        
        List<Integer> g = Arrays.asList(1, 2, 3 , 4);
        JavaRDD<Integer> gRDD = sc.parallelize(g);
        
        List<Integer> h = Arrays.asList(2, 4, 6);
        JavaRDD<Integer> hRDD = sc.parallelize(h);
        
        JavaRDD<Integer> iRDD = gRDD.subtract(hRDD);
        List<Integer> iOutput = iRDD.collect();
        System.out.println("Subtract Output***********************"+iOutput);
        
        // CARTESIAN
        
        List<Integer> j = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> jRDD = sc.parallelize(j);
        
        List<Integer> k = Arrays.asList(4,5);
        JavaRDD<Integer> kRDD = sc.parallelize(k);
        
        JavaPairRDD<Integer,Integer> lRDD = jRDD.cartesian(kRDD);
        
        lRDD.saveAsTextFile("out/cartesian-product");
       

	}

}
