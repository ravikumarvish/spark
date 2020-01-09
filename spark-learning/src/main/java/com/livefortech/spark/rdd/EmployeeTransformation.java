/**
 * 
 */
package com.livefortech.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



/**
 * @author ravikumarvish
 *
 */
public class EmployeeTransformation {

	/**
	 *  Problem statement is to find US employess. Put their name and gender
	 */
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Spark Transformation App").setMaster("local[1]");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> employees = context.textFile("in/employee.txt");
		JavaRDD<String> usEmployees = employees.filter(employee -> { 
			return employee.split("\t")[2].equals("US");
		});
		
		System.out.println("***********USEmployee"+usEmployees.count());
		
		JavaRDD<String> emplyeeNameWithAge  = usEmployees.map(employee -> {
			String[] records = employee.split("\t");
			System.out.println(records);
			return records[1] + "\t" + records[2] + "\t" + records[3] + "\t"+ records[4];
		});
		
		
		emplyeeNameWithAge.saveAsTextFile("out/USEmployee");
		
	}

}