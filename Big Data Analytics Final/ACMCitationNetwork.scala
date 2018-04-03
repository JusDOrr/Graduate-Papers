import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._

import scala.util.matching.Regex
import scala.util.hashing.MurmurHash3

object ACMCitationNetwork {
  def main(args: Array[String]): Unit =
  {
    if(args.length < 2)
    {
      System.err.println("Usage: ACMCitationNetwork <input file> <output file>")
      System.exit(1)
    }
    
    // Create Spark Session
    val myhivewarehouse = "hdfs://CSC570-BD-HMF12:9000/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("acm citation network app")
      .config("spark.sql.warehouse.dir", myhivewarehouse)
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()
      
    import spark.implicits._;
    val sc = spark.sparkContext;  
        
    // Hadoop Confiuguration
    @transient val config = new Configuration
    config.set("textinputformat.record.delimiter", "#*")    
        
    // ==================================================
    // Part 1 - Building the citation Network using Spark
    // ==================================================
    
    // Get File into starting format
    val inputRDD = sc.newAPIHadoopFile(args(0), classOf[TextInputFormat], classOf[LongWritable], classOf[Text], config)
    val inputRDDMapFilter = inputRDD.map{case(key,value)=>value.toString}.filter(value=>value.length!=0)
    
    // Lets store the paper id/title/references in a Table
    val paperData = inputRDDMapFilter.map{case(chunk)=>Paper(getIndexKey(chunk), chunk.split('#')(0).trim, getReferences(chunk))}
    val papersView = paperData.toDF.createOrReplaceTempView("papersView")
    
    // Select only the papers that reference others
    val pairData = spark.sql("SELECT paper_id, paper_references FROM papersView WHERE NOT paper_references='Null'")
    
    // Flatten the key values so we can create the graph
    val flatData = pairData.rdd.map(row => (row(0).toString,row(1).toString)).flatMapValues(refs=>refs.split(',')).map(pair=>PaperReference(pair._1,pair._2))
    
    //// Write to file in format: Paper_Id    Paper_Reference
    //val out1 = flatData.map{case PaperReference(id,ref)=>Seq(id,ref).mkString("\t")}
    //out1.saveAsTextFile(args(1) + "/links")
    
    // ===========================================
    // Part 2.1 - Calculate In-degree Distribution
    // ===========================================
    
    // Hashing index/references to long for graph creation
    val hashedData = flatData.map{case PaperReference(index, reference) => (MurmurHash3.stringHash(index).toLong, MurmurHash3.stringHash(reference).toLong)}   
    val graph = Graph.fromEdgeTuples(hashedData, null)    
    val vertCount = graph.numVertices
    
    val indegreeSums = graph.inDegrees.map{case(node,indegree)=>(indegree, 1)}.reduceByKey((v1,v2)=>v1+v2)
    val indegreeDist = indegreeSums.map{case(indegree,sum)=>(indegree,sum.toDouble/vertCount.toDouble)}
    
    //// Write to file in format: in-degree,distribution
    //val out2 = indegreeDist.map{case (indegree,dist)=>Seq(indegree,dist).mkString(",")}
    //out2.saveAsTextFile(args(1) + "/indegreeDist")
    
    // =============================
    // Part 2.2 - Weighted Page Rank
    // =============================
    
    // For each Node, calculate the inLinks and outLinks
    val wIn = flatData.map{case PaperReference(id,ref)=>(ref,1)}.reduceByKey((v1,v2)=>v1+v2).map{case(x,y)=>weightIn(x,y)}
    val wOut = flatData.map{case PaperReference(id,ref)=>(id,1)}.reduceByKey((v1,v2)=>v1+v2).map{case(x,y)=>weightOut(x,y)}
    
    // This stores the links with the numerator of their wIn/wOut calculations
    val topWeightsJoin = flatData.toDF.as("links").join(wIn.toDF.as("win"), $"links.paper_reference"===$"win.wIn_id")
                                                  .join(wOut.toDF.as("wout"), $"links.paper_reference"===$"wout.wOut_id")
                                                  .select($"paper_id", $"paper_reference", $"_wIn", $"_wOut")
    
    // These store the denominator for the wIn/wOut calculations
    val botIn = topWeightsJoin.rdd.map(row=>(row.getAs[String]("paper_id"), row.getAs[Int]("_wIn"))).reduceByKey((v1,v2)=>v1+v2).map{case(x,y)=>weightIn(x,y)}
    val botOut = topWeightsJoin.rdd.map(row=>(row.getAs[String]("paper_id"), row.getAs[Int]("_wOut"))).reduceByKey((v1,v2)=>v1+v2).map{case(x,y)=>weightOut(x,y)}
    
    // Links with their weightFactors
    val weightCalcs = topWeightsJoin.as("tw").join(botIn.toDF.as("bIn"), $"tw.paper_id"===$"bIn.wIn_id")
                                             .join(botOut.toDF.as("bOut"), $"tw.paper_id"===$"bOut.wOut_id")
                                             .select($"paper_id", $"paper_reference", (($"tw._wIn"/$"bIn._wIn")*($"tw._wOut"/$"bOut._wOut")).alias("weight"))
    
    // Place in "group-able" format
    val weights = weightCalcs.rdd.map(row=>(row.getAs[String]("paper_id"),(row.getAs[String]("paper_reference"),row.getAs[Double]("weight"))))                                        

    // Set our initial point ranks per node
    val links = weights.groupByKey().persist()
    var ranks = links.map{case (id,links)=>(id, 1/vertCount.toDouble)}
    
    // Calculate weighted ranks 10 times
    for(x <- 1 to 10){
      val contribs = links.join(ranks).flatMap{case(id,(links,rank))=>getWeightedRanks(links, rank)}
      ranks = contribs.reduceByKey((c1,c2)=>c1+c2).map{case(id,sum)=>(id, sum * .85 + .15/vertCount.toDouble)}
    }
    
    //Order ranks by point rank and select top 10
    val topTen = ranks.toDF.select($"_1", $"_2").sort($"_2".desc).limit(10)
    
    //Join papersView(for title), wIn (for in-links), and ranks (for page-rank)
    val answer = topTen.as("tt").join(paperData.toDF.as("papers"), $"tt._1"===$"papers.paper_id")
                                .join(wIn.toDF.as("inlinks"), $"papers.paper_id"===$"inlinks.wIn_id")
                                .select($"papers.paper_title", $"inlinks._wIn", $"tt._2")
                                .sort($"tt._2".desc)
          
    //// Write to file in format: Paper_Title    Num_Of_In-Links    Page_rank
    //val out3 = answer.rdd.map(row=>Seq(row(0),row(1),row(2)).mkString("\t"))
    //out3.saveAsTextFile(args(1) + "/topten")
                                
    spark.stop()
  }
  
  def getWeightedRanks(links:Iterable[(String,Double)], rank:Double):Iterable[(String,Double)]={
    // Here we want to return the rank * weight factor
    for(l <-links) yield (l._1, l._2 * rank)
  }
  
  def getIndexKey(line: String): String={
    val pattern = new Regex("#index[a-zA-Z0-9]*")
    val found = pattern.findFirstIn(line).getOrElse("Null")
    
    if(found!="Null")
    {
      val phrase = new Regex("#index")
      return phrase.replaceFirstIn(found, "")
    }
    else
      return found
  }
  
  def getReferences(line: String): String={
    val pattern = new Regex("#%[a-zA-Z0-9]*")
    val found = pattern.findAllIn(line).toList
    
    if(found.length > 0)
    {
      val refList = for(ref <- found) yield {ref.replace("#%","")}    
      return refList.mkString(",")
    }
    else
      return "Null"
  }
  
  case class Paper(paper_id:String, paper_title:String, paper_references:String)
  case class PaperReference(paper_id:String, paper_reference:String)
  
  case class weightIn(wIn_id:String, _wIn:Int)
  case class weightOut(wOut_id:String, _wOut:Int)
}










