import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphAnalysisEarly {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Community Collapse - Early Graph Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // ------------------------------------------------------------
    // HDFS configuration
    // ------------------------------------------------------------

    sc.hadoopConfiguration.set(
      "fs.defaultFS",
      "hdfs://localhost:9000"
    )

    println()
    println("=" * 70)
    println("       EARLY-PERIOD COMMUNITY GRAPH ANALYSIS")
    println("=" * 70)
    println()

    // ------------------------------------------------------------
    // Early period
    //
    // December 2013 -> August 2015
    // ------------------------------------------------------------

    val earlyStart = "2013-12-01 00:00:00"
    val earlyEnd   = "2015-08-31 23:59:59"

    println("Analysis period:")
    println(s"From : $earlyStart")
    println(s"To   : $earlyEnd")
    println()

    // ------------------------------------------------------------
    // Load the cleaned Pig output
    // ------------------------------------------------------------

    val inputPath =
      "/community_collapse/pig_output/reddit_cleaned/*"

    val rawData = sc.textFile(inputPath)

    // ------------------------------------------------------------
    // Parse cleaned records
    //
    // Format:
    // source
    // target
    // post_id
    // timestamp
    // sentiment
    // year
    // month
    // ------------------------------------------------------------

    val earlyRecords = rawData
      .map(_.split("\t"))
      .filter(parts => parts.length >= 7)
      .filter { parts =>
        val timestamp = parts(3)
        timestamp >= earlyStart && timestamp <= earlyEnd
      }
      .map { parts =>
        (
          parts(0),
          parts(1),
          parts(2),
          parts(3),
          parts(4).toInt,
          parts(5),
          parts(6)
        )
      }
      .cache()

    val recordCount = earlyRecords.count()

    println("=" * 70)
    println(s"Early-period interaction records: $recordCount")
    println("=" * 70)
    println()

    // ------------------------------------------------------------
    // Create weighted edges
    //
    // Multiple interactions between the same pair are aggregated.
    // ------------------------------------------------------------

    val rawEdges: RDD[(String, String, Int)] =
      earlyRecords
        .map {
          case (source, target, _, _, _, _, _) =>
            ((source, target), 1)
        }
        .reduceByKey(_ + _)
        .map {
          case ((source, target), weight) =>
            (source, target, weight)
        }
        .cache()

    val edgeCount = rawEdges.count()

    println(s"Unique early-period edges: $edgeCount")
    println()

    // ------------------------------------------------------------
    // Create vertex list
    // ------------------------------------------------------------

    val verticesRDD: RDD[String] =
      rawEdges
        .flatMap {
          case (source, target, _) =>
            Seq(source, target)
        }
        .distinct()
        .cache()

    val vertexCount = verticesRDD.count()

    println(s"Early-period vertices: $vertexCount")
    println()

    // ------------------------------------------------------------
    // Assign numerical IDs to vertices
    // ------------------------------------------------------------

    val vertexIds: RDD[(String, Long)] =
      verticesRDD
        .zipWithUniqueId()
        .cache()

    // ------------------------------------------------------------
    // Build GraphX edges
    // ------------------------------------------------------------

    val sourceWithIds =
      rawEdges
        .map {
          case (source, target, weight) =>
            (source, (target, weight))
        }
        .join(vertexIds)
        .map {
          case (source, ((target, weight), sourceId)) =>
            (target, (sourceId, weight))
        }

    val edges: RDD[Edge[Int]] =
      sourceWithIds
        .join(vertexIds)
        .map {
          case (target, ((sourceId, weight), targetId)) =>
            Edge(sourceId, targetId, weight)
        }
        .cache()

    // ------------------------------------------------------------
    // Build GraphX graph
    // ------------------------------------------------------------

    val graph: Graph[String, Int] =
      Graph(
        vertexIds.map {
          case (name, id) =>
            (id, name)
        },
        edges
      ).cache()

    println("=" * 70)
    println("GRAPH CREATED")
    println("=" * 70)

    println(s"Vertices : ${graph.numVertices}")
    println(s"Edges    : ${graph.numEdges}")
    println()

    // ------------------------------------------------------------
    // Degree calculations
    // ------------------------------------------------------------

    val inDegrees =
      graph.inDegrees

    val outDegrees =
      graph.outDegrees

    val totalDegrees: RDD[(VertexId, Int)] =
      inDegrees
        .fullOuterJoin(outDegrees)
        .mapValues {
          case (in, out) =>
            in.getOrElse(0) + out.getOrElse(0)
        }

    // ------------------------------------------------------------
    // Average degrees
    // ------------------------------------------------------------

    val avgInDegree =
      inDegrees.values
        .map(_.toDouble)
        .mean()

    val avgOutDegree =
      outDegrees.values
        .map(_.toDouble)
        .mean()

    val avgDegree =
      totalDegrees.values
        .map(_.toDouble)
        .mean()

    println("=" * 70)
    println("DEGREE STATISTICS")
    println("=" * 70)

    println(f"Average in-degree  : $avgInDegree%.2f")
    println(f"Average out-degree : $avgOutDegree%.2f")
    println(f"Average degree     : $avgDegree%.2f")
    println()

    // ------------------------------------------------------------
    // Top communities by out-degree
    // ------------------------------------------------------------

    println("=" * 70)
    println("TOP 10 COMMUNITIES BY EARLY OUT-DEGREE")
    println("=" * 70)

    graph.outDegrees
      .join(graph.vertices)
      .map {
        case (_, (degree, name)) =>
          (degree, name)
      }
      .sortByKey(false)
      .take(10)
      .foreach {
        case (degree, name) =>
          println(f"$name%-35s $degree")
      }

    println()

    // ------------------------------------------------------------
    // Top communities by in-degree
    // ------------------------------------------------------------

    println("=" * 70)
    println("TOP 10 COMMUNITIES BY EARLY IN-DEGREE")
    println("=" * 70)

    graph.inDegrees
      .join(graph.vertices)
      .map {
        case (_, (degree, name)) =>
          (degree, name)
      }
      .sortByKey(false)
      .take(10)
      .foreach {
        case (degree, name) =>
          println(f"$name%-35s $degree")
      }

    println()

    // ------------------------------------------------------------
    // PageRank
    // ------------------------------------------------------------

    println("=" * 70)
    println("CALCULATING EARLY-PERIOD PAGERANK")
    println("=" * 70)
    println()

    val pageRankGraph =
      graph.pageRank(0.0001)

    println("Top 10 communities by early PageRank:")
    println()

    pageRankGraph.vertices
      .join(graph.vertices)
      .map {
        case (_, (rank, name)) =>
          (rank, name)
      }
      .sortByKey(false)
      .take(10)
      .foreach {
        case (rank, name) =>
          println(f"$name%-35s $rank%.6f")
      }

    println()

    // ------------------------------------------------------------
    // Connected components
    // ------------------------------------------------------------

    println("=" * 70)
    println("CONNECTED COMPONENT ANALYSIS")
    println("=" * 70)
    println()

    val components =
      graph.connectedComponents()

    val componentCount =
      components.vertices
        .map {
          case (_, componentId) =>
            componentId
        }
        .distinct()
        .count()

    println(s"Connected components: $componentCount")
    println()

    // ------------------------------------------------------------
    // Export degree results
    // ------------------------------------------------------------

    val degreeOutput =
      totalDegrees
        .join(graph.vertices)
        .map {
          case (_, (degree, name)) =>
            s"$name\t$degree"
        }

    degreeOutput
      .coalesce(2)
      .saveAsTextFile(
        "/community_collapse/graph/results/early_degree"
      )

    // ------------------------------------------------------------
    // Export PageRank results
    // ------------------------------------------------------------

    val pagerankOutput =
      pageRankGraph.vertices
        .join(graph.vertices)
        .map {
          case (_, (rank, name)) =>
            s"$name\t$rank"
        }

    pagerankOutput
      .coalesce(2)
      .saveAsTextFile(
        "/community_collapse/graph/results/early_pagerank"
      )

    // ------------------------------------------------------------
    // Export connected components
    // ------------------------------------------------------------

    val componentOutput =
      components.vertices
        .join(graph.vertices)
        .map {
          case (_, (componentId, name)) =>
            s"$name\t$componentId"
        }

    componentOutput
      .coalesce(2)
      .saveAsTextFile(
        "/community_collapse/graph/results/early_components"
      )

    // ------------------------------------------------------------
    // Finish
    // ------------------------------------------------------------

    println("=" * 70)
    println("EARLY GRAPH ANALYSIS COMPLETE")
    println("=" * 70)

    println()
    println("Results written to:")
    println("  /community_collapse/graph/results/early_degree")
    println("  /community_collapse/graph/results/early_pagerank")
    println("  /community_collapse/graph/results/early_components")
    println()

    spark.stop()
  }
}
