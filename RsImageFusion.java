/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import scala.Tuple2;
import scala.Tuple4;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.DataFrameReader;
import java.nio.ByteBuffer;
import java.io.Closeable;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.function.PairFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.math.*; 
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.function.FilterFunction;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import static java.util.Arrays.asList;
import org.apache.spark.sql.Column;
import scala.reflect.ClassTag;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaSparkContext;

public final class RsImageFusion {

  
	
  //calculate mean value for a big image
  private static float calculateMean(SparkSession spark,Dataset<Row> data,long rowcount, long columncount){
	  
	   Dataset<Float> floatDS= data.map(
        new MapFunction<Row,Float>() {
           @Override
           public Float call(Row row){
             int elementscount = row.size()-1;
              Float sum=0.0f;
             for(int i=0;i<elementscount;i++){
                Float tmpf = Float.parseFloat(row.get(i).toString());
                sum = sum+tmpf;
             }
             return new Float(sum);
           } 
        },  Encoders.FLOAT()); 
		JavaRDD<Float> rddfloat = floatDS.toJavaRDD();
		Float sum = rddfloat.reduce((a,b)->a+b); 
		float avg = sum/(rowcount*columncount);	  
		return avg;
  }
  
	
	
  //calculate stddev for a big image	
  private static Double claulatstddev(SparkSession spark,Dataset<Row> data,int rowcount, int columncount){
  
	Broadcast<Integer> bbcolumncount= spark.sparkContext().broadcast(columncount,  scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
		
       Dataset<Float> floatDS= data.map(
        new MapFunction<Row,Float>() {
           @Override
           public Float call(Row row){
             int elementscount = bbcolumncount.getValue();
              Float sum=0.0f;
             for(int i=0;i<elementscount;i++){
				 
                Float tmpf = Float.parseFloat(row.get(i).toString());
				if(0.0f==tmpf){
					continue;
				}
                sum = sum+tmpf;
             }
             return new Float(sum);
           } 
        },  Encoders.FLOAT()); 

        Float sum = floatDS.reduce((a,b)->a+b); 
        Float b_avg = sum/(float)(rowcount*columncount);
        
        //broadcasting the sum of the image 
        Broadcast<Float> bb_avg= spark.sparkContext().broadcast(b_avg,  scala.reflect.ClassTag$.MODULE$.apply(Float.class));

        Dataset<Float> floatDS1= data.map(
        new MapFunction<Row,Float>() {
           @Override
           public Float call(Row row){
             int elementscount =  bbcolumncount.getValue();
              float sumxx=0.0F;
             for(int i=0;i<elementscount;i++){
                float tmpf1 = Float.parseFloat(row.get(i).toString())-bb_avg.getValue();
				if(0.0f==tmpf1){
					continue;
				}
                sumxx = sumxx+tmpf1*tmpf1;
             }
             return new Float(sumxx);
           } 
        },  Encoders.FLOAT()); 

 
      float sumxx = floatDS1.reduce((a,b)->a+b); 	// the reduce operation would cost a lot of time, please take care 
        
      Double s = Math.sqrt (sumxx/(rowcount*columncount));
      return s;
  }

 



  private static Dataset<Row> joinAllPairs(SparkSession spark, Dataset<Row> fine1, Dataset<Row> fine2, Dataset<Row>  coarse1, 	Dataset<Row>  coarse2, Dataset<Row>  coarse0){

		Dataset<Tuple2<Integer, byte[] > >  fine1DS= fine1.map(
        new MapFunction<Row,Tuple2< Integer, byte[] > >() {
           @Override
           public Tuple2< Integer, byte[] >  call(Row row){
			 int rowNumber = Integer.parseInt(row.get(row.size()-1).toString());
			 byte[] zz = new byte[(row.size()-1)*4];		
			 for(int i=0;i<row.size()-1;i++){ 
				int tmpint =(int)row.getDouble(i); //combine all columns into byte array 								
				byte[] bytes = ByteBuffer.allocate(4).putInt(tmpint).array();
				for(int j=0;j<bytes.length;j++){
					zz[i*4+j]=bytes[j];
				}		
			 }
             return new Tuple2<Integer, byte[]>(rowNumber, Arrays.copyOfRange(zz,0,zz.length));
           } 
        },  Encoders.tuple(Encoders.INT(), Encoders.BINARY()));
		
		Dataset<Row> df1 = fine1DS.toDF("rowKey","value1");
		df1.createOrReplaceTempView("fine1DS");
		spark.sql("REFRESH TABLE fine1DS");	
		
		 Dataset<Tuple2<Integer, byte[] > >  fine2DS= fine2.map(
        new MapFunction<Row,Tuple2< Integer, byte[] > >() {
           @Override
           public Tuple2< Integer, byte[] >  call(Row row){
			 int rowNumber = Integer.parseInt(row.get(row.size()-1).toString());
			 byte[] zz = new byte[(row.size()-1)*4];		
			 for(int i=0;i<row.size()-1;i++){ 
				int tmpint =(int)row.getDouble(i);
				byte[] bytes = ByteBuffer.allocate(4).putInt(tmpint).array();
				for(int j=0;j<bytes.length;j++){
					zz[i*4+j]=bytes[j];
				}		
			 }
             return new Tuple2<Integer, byte[]>(rowNumber, Arrays.copyOfRange(zz,0,zz.length));
           } 
        },  Encoders.tuple(Encoders.INT(), Encoders.BINARY()));	
		
		Dataset<Row> df2 = fine2DS.toDF("rowKey","value2");
		df2.createOrReplaceTempView("fine2DS");
		spark.sql("REFRESH TABLE fine2DS");	


		Dataset<Tuple2<Integer, byte[] > >  coarse1DS= coarse1.map(
        new MapFunction<Row,Tuple2< Integer, byte[] > >() {
           @Override
           public Tuple2< Integer, byte[] >  call(Row row){
			 int rowNumber = Integer.parseInt(row.get(row.size()-1).toString());
			 byte[] zz = new byte[(row.size()-1)*4];		
			 for(int i=0;i<row.size()-1;i++){ 
				int tmpint =(int)row.getDouble(i);
				byte[] bytes = ByteBuffer.allocate(4).putInt(tmpint).array();
				for(int j=0;j<bytes.length;j++){
					zz[i*4+j]=bytes[j];
				}		
			 }
             return new Tuple2<Integer, byte[]>(rowNumber, Arrays.copyOfRange(zz,0,zz.length));
           } 
        },  Encoders.tuple(Encoders.INT(), Encoders.BINARY()));
		
		Dataset<Row> df3 = coarse1DS.toDF("rowKey","value3");
		df3.createOrReplaceTempView("coarse1DS");
		spark.sql("REFRESH TABLE coarse1DS");	

		Dataset<Tuple2<Integer, byte[] > >  coarse2DS= coarse2.map(
        new MapFunction<Row,Tuple2< Integer, byte[] > >() {
           @Override
           public Tuple2< Integer, byte[] >  call(Row row){
			 int rowNumber = Integer.parseInt(row.get(row.size()-1).toString());
			 byte[] zz = new byte[(row.size()-1)*4];		
			 for(int i=0;i<row.size()-1;i++){ 
				int tmpint =(int)row.getDouble(i);
				byte[] bytes = ByteBuffer.allocate(4).putInt(tmpint).array();
				for(int j=0;j<bytes.length;j++){
					zz[i*4+j]=bytes[j];
				}		
			 }
             return new Tuple2<Integer, byte[]>(rowNumber, Arrays.copyOfRange(zz,0,zz.length));
           } 
        },  Encoders.tuple(Encoders.INT(), Encoders.BINARY()));
		
		
	Dataset<Row> df4 = coarse2DS.toDF("rowKey","value4");
	df4.createOrReplaceTempView("coarse2DS");
	spark.sql("REFRESH TABLE coarse2DS");	

	Dataset<Tuple2<Integer, byte[] > >  coarse0DS1= coarse0.map(
	new MapFunction<Row,Tuple2< Integer, byte[] > >() {
	@Override
	public Tuple2< Integer, byte[] >  call(Row row){
		int rowNumber = Integer.parseInt(row.get(row.size()-1).toString());
		byte[] zz = new byte[(row.size()-1)*4];		
		for(int i=0;i<row.size()-1;i++){ 
		int tmpint =(int)row.getDouble(i);
		byte[] bytes = ByteBuffer.allocate(4).putInt(tmpint).array();
		for(int j=0;j<bytes.length;j++){
		zz[i*4+j]=bytes[j];
			}		
		}
			return new Tuple2<Integer, byte[]>(rowNumber, Arrays.copyOfRange(zz,0,zz.length));
		} 
	},  Encoders.tuple(Encoders.INT(), Encoders.BINARY()));
		Dataset<Row> df5 = coarse0DS1.toDF("rowKey","BBB");
		df5.createOrReplaceTempView("coarse0DS1");
		spark.sql("REFRESH TABLE coarse0DS1"); 
		
		String finalStr = "SELECT coarse0DS1.rowKey, value1,value2, value3, value4, BBB  FROM  coarse0DS1  INNER JOIN fine1DS ON coarse0DS1.rowKey=fine1DS.rowKey   INNER JOIN fine2DS  ON coarse0DS1.rowKey=fine2DS.rowKey  INNER JOIN coarse1DS  ON coarse0DS1.rowKey=coarse1DS.rowKey  INNER JOIN coarse2DS  ON coarse0DS1.rowKey=coarse2DS.rowKey";
		Dataset<Row> df6 =spark.sql(finalStr).toDF("rowKey","value1","value2","value3","value4","BBB");
		df6.createOrReplaceTempView("Ftemp");
		spark.sql("REFRESH TABLE Ftemp");	
		return df6;
  } 
 
  
  public static void main(String[] args) throws Exception {

 
    SparkSession spark = SparkSession
      .builder()
	.appName("JavaWordCount")
    .getOrCreate();
    int num_class=4;
    int num_partitons=20;
    int windowSize =5; 			// the window size should be changed according to the requirement, user should look at the udaf defined below	
	Dataset<Row>  coarse0 = spark.read().parquet(args[0]); 
	Dataset<Row>  fine1 = spark.read().parquet(args[1]);
	Dataset<Row>  fine2 = spark.read().parquet(args[2]);  
	Dataset<Row>  coarse1 = spark.read().parquet(args[3]);
	Dataset<Row>  coarse2= spark.read().parquet(args[4]); 
	Double[] similar_th = new Double[2];
	long rowcount = fine1.count();
	Object[] tmpobjArr = scala.collection.JavaConverters.seqAsJavaListConverter(fine1.first().toSeq()).asJava().toArray();
	int  columnscount =fine1.columns().length-1;  //Sine the last column is "__index_level_0__ "  the subtraction 1 is required
	similar_th[0]= claulatstddev(spark,fine1,(int)rowcount,columnscount-1)*2.0/num_class; //The last column is the row key of the images     
	similar_th[1]= claulatstddev(spark,fine2,(int)rowcount,columnscount-1)*2.0/num_class;
	Dataset<Row> all_data = joinAllPairs(spark, fine1, fine2,coarse1,coarse2, coarse0 ).toDF("rowKey","value1","value2","value3","value4","BBB");
	all_data.createOrReplaceTempView("Ftemp");
	spark.sql("REFRESH TABLE Ftemp");	
	spark.udf().register("RSImageFusionUDAF", new RSImageFusionUDAF(similar_th[0],similar_th[1],windowSize));
	all_data.repartitionByRange(num_partitons, new Column("rowKey")).sortWithinPartitions(new Column("rowKey")).cache();
	String sqlString= "SELECT rowKey, RSImageFusionUDAF("+"rowKey"+", "+"value1"+ ", "+ " value2"+ ", "+ "value3"+ ", "+ "value4"+ ", "+" BBB" + ") OVER (ORDER BY rowKey "+" ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING )  FROM Ftemp";
	Dataset<Row> df =spark.sql(sqlString).toDF("rowKey","FinalPixels");
	df.createOrReplaceTempView("BBtemp");
	spark.sql("REFRESH TABLE BBtemp");
	df.show();
    spark.stop();
    }
}
