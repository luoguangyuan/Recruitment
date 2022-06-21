# data analyst candidate assignment 

please init a dataframe with 10 million rows of data: index, a random number 
then find the top 100 biggest number

now there are 1 billion rows of data, try to find the top 100 biggest number
 
please give your solutions (code/notebook) of the cases above, and provided with how much time in secs , spec of your machine with your solution

/**首先使用java random生成1000万个随机数，在生成随机数的同时，在随机数前加上前缀，前缀在后面会改成索引**/
package mianshi;

import java.io.*;
import java.util.Random;

public class Demo1produce {
    public static void main(String[] args) throws IOException {
        Random rd = new Random();
        int j;
        BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream("D:\\produce1.txt",true))
        );

        for (j=1;j<=10000000;j++)
        {
            int i = rd.nextInt(10000000);
            System.out.println(i);
            bw.write(j+"\t"+i+",");
        }
        bw.close();
    }
}

/**====================================================================================================================================**/

package mianshi


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object Demotest7 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("number")
      .master("local[2]")
      .config("spark.sql.shuffle.partition", 1)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

val sc: SparkContext = spark.sparkContext
    val lineRDD: RDD[String] = sc.textFile("data/produce1.txt")
    val wordRDD: RDD[String] = lineRDD.flatMap((line: String) => line.split(","))
    /**通过split切分将原来的一列切成两列**/
    val linesRDD: RDD[(String, String)] = wordRDD.map({ i => (i.split("\t")(0), i.split("\t")(1)) })
    /**转换成DF格式，并赋予表结构，前面的前缀变成索引，可以通过id获取对应的数**/
    val linesDF: DataFrame = linesRDD.toDF("id", "num")

    linesDF.show()
    /**这里完成了第一个的任务**/
/**============================================================================================================================================**/
    //注册一张表
    linesDF.createOrReplaceTempView("produce")
    
    /**下面的sql语句对生成的1000万个数进行排序求出前100个最大的数**/
    /**使用窗口函数对num进行排序**/
    val produceDF: DataFrame = spark.sql(
      """
        |select id,num,row_number() over(order by num desc) as order from produce
        |
        |""".stripMargin)

    produceDF.show(100)

  }
}
/**这里完成第二个任务**/
/**===========================================================================================================================================**/
/**第三个任务分两种情况：1、当资源充裕的时候 2、当资源比较少的时候**/
/**1、资源足够**/
select * from biao order by num desc limit 100
/**直接从数据中获取前100个最大的数**/
/**======================================================================**/

/**2、资源不充足时，这里不建议直接使用sql查询，用时间换空间，    
使用堆排序的方法
**/
package mianshi;
import java.io.*;
public class Demo2reader {
    public static void main(String[] args) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader("data/produce.txt"));
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        String ls = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null)
        { stringBuilder.append(line);
            stringBuilder.append(ls);
            }
        // 删除最后一个新行分隔符
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        reader.close();
        String content = stringBuilder.toString();
        String[] strArr = content.split(",")    
        int[] ints = new int[strArr.length];
        
        for(int i=0;i<strArr.length;i++){
            ints[i] = Integer.parseInt(strArr[i]);
        }
        }
        
        int[100] arr;
        for(k=0;k++;k<100){
        arr[k]=ints[k]
        }
        //冒泡排序
        for(var i = 0;i<arr.length-1;i++) {
            for(var j = 0;j<arr.length-i-1;j++){
                if(arr[j]>arr[j+1]){
                    //把大的数字放到后面
                    var str = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = str;
                }
            }
        }
        for(var m =100;m<ints.lenth-1;m++){
        if(ints[m]>arr[0]){
        arr[0]=ints[m]
        for(var i = 0;i<arr.length-1;i++) {
            for(var j = 0;j<arr.length-i-1;j++){
                if(arr[j]>arr[j+1]){
                    //把大的数字放到后面
                    var str = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = str;
                }
            }
        }
        }
        }
   }
  }
  
  /**通过最小堆排序获取最大的100个数**/
  
  /**=======================================**/






