import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io._

class FastaReader(val filename: String) extends Iterator[(String, String, String)] {
  private lazy val reader = new BufferedReader(new FileReader(filename))

  class FastaReadException(string: String) extends Exception(string)
  def hasNext(): Boolean = reader.ready
  def next(): (String, String, String) = {
    // Read the tag line
    val tag = reader.readLine
    if (tag(0) != '>')
      throw new FastaReadException("record start expected")
    var sequencelist = ""
    // Read the sequence body
    do {
      reader.mark(512) // 512 is sufficient for a single tag line
      val line = reader.readLine
      if (line(0) != '>') sequencelist += line
      if (!reader.ready || line(0) == '>') {
        // Reached the end of the sequence
        if (reader.ready) reader.reset()
        // Remove prepending '>'
        val tag2 = tag.drop(1).trim
        val id = tag2.split(Array(' ', '\t'))(0)
        return (id, tag2, sequencelist)
      }
    } while (reader.ready)
    // should never reach this...
    throw new FastaReadException("Error in file " + filename + " (tag=" + tag + ")")
  }
}

class NucleotideSequence(val filename: String){
  def read(position: Int): Option[Array[Char]]={
    val parsed = new FastaReader(filename)
    val sequences = parsed.map(x=>x._3)
    sequences.slice(position, position + 1).toList.headOption match{
      case None => None
      case Some(seq) => Some(seq.toArray)
    }
  }
}

trait sequenceDistance[A]{
  def penaltyLeft: Int
  def penaltyUp: Int
  def select3(a: Int, b: Int, c: Int): Int
  def penaltyBuilder(size:Int): Array[Int]
  def Compute [B >: A, C>: A](s1: Array[B], s2: Array[C]): (Int, Option[Double], Int, Int) ={
    val firstLineDistances = penaltyBuilder(s2.length)
    val firstColumnDistances = penaltyBuilder(s1.length)
    val seqAndDist = s1.zip(firstColumnDistances)

    def partialDist[D >: A](left: (Int, Int, Int), up:(Int, Int, Int), diag: (Int, Int, Int), c1: D, c2: D): (Int, Int, Int) = {
      val selected = select3(left._1+penaltyLeft, up._1+penaltyUp, diag._1+Compare(c1, c2))	//si pensi min
      if (selected == diag._1+Compare(c1, c2) && c1 == c2){
        val matched=diag._2+1
        val different=diag._3
        return (selected, matched, different)
      }
      if (selected == diag._1+Compare(c1, c2) && c1 != c2){
        val matched=diag._2
        val different=diag._3+1
        return (selected, matched, different)
      }
      if (selected == up._1+penaltyUp){
        val matched = up._2
        val different = up._3
        (selected, matched, different)
      }
      else //if (selected == left._1+penaltyLeft)
      {
        val matched = left._2
        val different = left._3
        (selected, matched, different)
      }
    }

    def makeNewLine[D >:A, E>:A](oldLine: Array[(Int, Int, Int)], charLine: Array[E], leftDistance: Int, leftChar: D, diag: Int): Array[(Int, Int, Int)] = {
      val charsAndDists = oldLine.zip(charLine).scanLeft(((leftDistance, 0, 0), (diag, 0, 0)))( (acc, item) => (partialDist(acc._1, item._1, acc._2, leftChar, item._2), item._1)) //il secondo elemento è la diagonale, è ottenuto ad ogni passo mettendo item._1, quindi l'elemento che attualmente si trova esattamente sopra
      charsAndDists.drop(1).map(x=> x._1)
    }


    val result= seqAndDist.foldLeft((firstLineDistances, Array.fill(firstLineDistances.length)(0), Array.fill(firstLineDistances.length)(0)).zipped.toArray)((acc, item)=> makeNewLine(acc, s2, item._2, item._1, if(acc(0)._1==0) 0 else item._2)) //l'if serve per il primissimo valore, quello in M[0,0]

    val p= if (result(s2.length-1)._2+result(s2.length-1)._3!=0) Some(result(s2.length-1)._3.toDouble/(result(s2.length-1)._2+result(s2.length-1)._3).toDouble) else None
    (result(s2.length-1)._1, p, result(s2.length-1)._3, result(s2.length-1)._2); //1: score, 2: p-distance, 3: sostituzioni, 4: corretti
  }
  def Compare [B>: A](m1: B, m2: B): Int

  def Score[B >: A, C>: A](s1: Array[B], s2: Array[C]): Int ={Compute(s1, s2)._1}
  def pDistance[B >: A, C>: A](s1: Array[B], s2: Array[C]): Option[Double]={Compute(s1, s2)._2}
  def substitutions[B >: A, C>: A](s1: Array[B], s2: Array[C]): Int={Compute(s1, s2)._3}
  def matching[B >: A, C>: A](s1: Array[B], s2: Array[C]): Int={Compute(s1, s2)._4}
}

class lDistance extends sequenceDistance[Char]{
  def penaltyLeft = 1
  def penaltyUp = 1
  def penaltyBuilder(size:Int): Array[Int] = (0 until size).toArray
  def select3(a: Int, b: Int, c: Int): Int = Math.min(Math.min(a, b), c)
  def Compare [B>: Char](c1: B, c2: B): Int = {if (c1==c2) 0 else 1}
}

class NeighbourJoining {

  var r_i: Map[Int, Double] = Map[Int, Double]()
  var distances: Map[(Int, Int), Double] = Map[(Int, Int), Double]()
  var d_i_j: Map[(Int, Int), Double] = Map[(Int, Int), Double]()
  var graph: Map[(Int, Int), Double] = Map[(Int, Int), Double]()
  var numOfSeq: Int = 0
  var numOfNodeTmp : Int = 0
  var last_node: Int = 0

  def NJ() : Unit = {
    var n : (Int, Int) = (0, 0)
    for (_ <- 0 until (numOfSeq - 2)){
      setR_I()
      setD_I_J()
      n = joinSmallestNodes() //remove updateMatrix here
      updateMatrix(n._1, n._2)
    }
    graph += ((distances.filterKeys(k=> k._1<k._2).head._1._1, distances.filterKeys(k=> k._1<k._2).head._1._2) -> distances.filterKeys(k=> k._1<k._2).head._2)
  }


  def init(d: Map[(Int, Int), Double]): Unit = {
    // make matrix communative
    for ((k,v) <- d) {
      distances += ((k._1, k._2) -> v)
      distances += ((k._2, k._1) -> v)
    }

    numOfSeq = distances.max._1._1 + 1
    numOfNodeTmp = distances.max._1._1 + 1

    last_node = numOfSeq

    // init r_i can be half of that? |r_i| = num_seq
    for (i <- 0 until numOfSeq) {
      distances += ((i, i) -> 0)
    }
  }


  def setR_I(): Unit ={
    r_i=distances.groupBy(x=> x._1._1 ).mapValues(x=>x.foldLeft(0.0)((e, x)=> e+x._2)).mapValues(x=> x/(numOfSeq - 2))
  }


  def setD_I_J(): Unit ={
    val tmpMatrixDist: Map[(Int, Int), Double] = Map()++distances
    d_i_j = tmpMatrixDist.filterKeys(k=> k._1>k._2).transform((k, v) => v - r_i(k._1) - r_i(k._2))
  }

  def joinSmallestNodes(): (Int, Int) ={
    val min = d_i_j.minBy(_._2)

    val node_1 : Int = min._1._1
    val node_2 : Int = min._1._2

    val dist_node1: Double = 0.5 * distances(node_1, node_2) + 0.5 * (r_i(node_1) - r_i(node_2))
    val dist_node2: Double = 0.5 * distances(node_1, node_2) + 0.5 * (r_i(node_2) - r_i(node_1))
    graph += ((node_1, last_node) -> dist_node1)
    graph += ((node_2, last_node) -> dist_node2)
    (node_1, node_2)
  }

  def updateMatrix(node_1: Int, node_2: Int): Unit ={
    val x = distances.filterKeys(k => k._1 != node_1 && k._1 != node_2)
      .groupBy(x=>x._1._1).flatMap(
      x =>  {
        val d = (x._2.filter(_._1._2==node_1).head._2 + x._2.filter(_._1._2==node_2).head._2- distances(node_1, node_2))/2
        Seq(((x._1, last_node), d),((last_node, x._1), d))
      })

    distances = (distances.filterKeys(k => k._1 != node_1 && k._1 != node_2 && k._2 != node_1 && k._2 != node_2) ++ x) +((last_node, last_node)->0.0)
    numOfSeq-=1
    last_node = last_node + 1
  }
}

class ParNeighbourJoining(sc: SparkContext) {

  var r_i: Map[Int, Double] = Map[Int, Double]()
  var distances : RDD[((Int, Int), Double)]= sc.emptyRDD[((Int, Int), Double)]
  var d_i_j : RDD[((Int, Int), Double)]= sc.emptyRDD[((Int, Int), Double)]
  var graph : RDD[((Int, Int), Double)]= sc.emptyRDD[((Int, Int), Double)]
  //var numOfSeq: Int = 0
  var numOfNodeTmp : Int = 0
  var last_node: Int = 0

  def NJ(d: Map[(Int, Int), Double]) : Unit = {

    var r_i = Map[Int, Double]()
    //var distances : RDD[((Int, Int), Double)]= sc.emptyRDD[((Int, Int), Double)];
    //var d_i_j : RDD[((Int, Int), Double)]= sc.emptyRDD[((Int, Int), Double)];

    distances ++= sc.parallelize(d.toSeq).flatMap[((Int, Int), Double)]((x: ((Int, Int), Double))=> Seq[((Int, Int), Double)](((x._1._1, x._1._2), x._2), ((x._1._2, x._1._1), x._2)))
    val numOfSeq = distances.max._1._1 + 1
    numOfNodeTmp = distances.max._1._1 + 1
    last_node = numOfSeq
    distances.union(sc.parallelize(0 until numOfSeq).map(x=> (x, x) -> 0.0))


    (0 until (numOfSeq - 2)).foreach (i =>{

      r_i=distances.groupBy(x=> x._1._1 ).mapValues(x=>x.foldLeft(0.0)((e, x)=> e+x._2)).mapValues(x=> x/(numOfSeq-i - 2)).collect().toMap
      d_i_j = distances.filter(x=> x._1._1>x._1._2).map(x => (x._1 ,x._2 - r_i(x._1._1) - r_i(x._1._2)))


      val min = d_i_j.min()(Ordering[Double].on[((Int, Int),Double)](_._2))

      val node_1 : Int = min._1._1
      val node_2 : Int = min._1._2

      val dist_node1: Double = 0.5 * distances.lookup((node_1, node_2)).head + 0.5 * (r_i(node_1) - r_i(node_2))
      val dist_node2: Double = 0.5 * distances.lookup((node_1, node_2)).head + 0.5 * (r_i(node_2) - r_i(node_1))
      graph ++= sc.parallelize(Seq(((node_1, numOfSeq+i) , dist_node1)))
      graph ++= sc.parallelize(Seq(((node_2, numOfSeq+i) , dist_node2)))

      val dist = distances.lookup(node_1, node_2).head

      val x = distances.filter(x => x._1._1 != node_1 && x._1._1 != node_2)
        .groupBy((x: ((Int, Int), Double)) =>x._1._1).flatMap(
        x =>  {
          val d = (x._2.filter(_._1._2==node_1).head._2 + x._2.filter(_._1._2==node_2).head._2- dist)/2
          Seq(((x._1, numOfSeq+i), d),((numOfSeq+i, x._1), d))
        })
      distances = (distances.filter(x => x._1._1 != node_1 && x._1._1 != node_2 && x._1._2 != node_1 && x._1._2 != node_2) ++ x) ++ sc.parallelize(Seq((last_node, last_node)->0.0))
    })
    graph ++=sc.parallelize(Seq((distances.filter(x=> x._1._1<x._1._2).first()._1._1, distances.filter(x=> x._1._1<x._1._2).first()._1._2) -> distances.filter(x=> x._1._1<x._1._2).first()._2))
  }
}

object DDD extends Serializable {
  def computeD(f: (Array[Char], Array[Char]) => Double)(x: ((Int, Array[Char]), (Int, Array[Char]))): ((Int, Int), Double) = {
    ((x._1._1, x._2._1), f(x._1._2, x._2._2))
  }

  def computePDist(tuple: ((Int, Array[Char]), (Int, Array[Char]))): ((Int, Int), Double) = computeD(f = (s1, s2) => {
    val ld = new lDistance
    ld.pDistance(s1, s2) match {
      case None => 1.0
      case Some(d) => d
    }
  })(tuple)

  def computeSubsDist(tuple: ((Int, Array[Char]), (Int, Array[Char]))): ((Int, Int), Double) =  computeD((s1, s2)=>{
    val ld = new lDistance; ld.substitutions(s1, s2)})(tuple)
}



class Controller(par_matrix: Boolean,
                 par_joining: Boolean,
                 metric: String,
                 filedirs: Seq[String],
                 max_seq_per_file: Integer,
                 sc: SparkContext
                ) {

  def run ()={
    val files: Seq[String] = List("COVID-19_seqLunghe/alpha/1646989737406.sequences.fasta", "COVID-19_seqLunghe/beta/1646989945496.sequences.fasta", "COVID-19_seqLunghe/gamma/1646990274551.sequences.fasta")//filedirs.flatMap(z => new java.io.File(z).listFiles.filter(_.getName.endsWith(".fasta")).map(x=>z+"/"+x.getName))
    val data: Seq[(Array[Char], Array[Char], Array[Char])] = files.flatMap(x=> new FastaReader(x).take(max_seq_per_file).map(z=> (z._1.toArray, z._2.toArray, z._3.toArray)))
    println(files)
  //  println(data.size)
    //data._1 = id
    //data._2 = tag \in id
    //data._3 = sequence

    //val sequences = files.map((x)=> new NucleotideSequence(x).read(0))

    var pairs : List[((Int, Array[Char]), (Int, Array[Char]))] = List[((Int, Array[Char]), (Int, Array[Char]))]()

    for (i <- data.indices){
      for (j<- 0 until i){
        pairs = pairs :+ ((i, data(i)._3), (j, data(j)._3))
      }
    }

    var distances = Map[(Int, Int), Double]()
    var dist_time : Long = 0
    var nj_time : Long = 0
    var graph = Map[(Int, Int), Double]()

    if (par_matrix) {
      val ppairs = sc.parallelize(pairs)
      if (metric == "substitutions") {
        val t0 = System.nanoTime()
        distances = ppairs.map((x: ((Int, Array[Char]), (Int, Array[Char]))) => DDD.computeSubsDist(x)).collect().toMap
        val t1 = System.nanoTime()
        dist_time=(t1 - t0)/1000000
      }
      if (metric == "p") {
        val t0 = System.nanoTime()
        distances = ppairs.map(x => DDD.computePDist(x)).collect().toMap
        val t1 = System.nanoTime()
        dist_time=(t1 - t0)/1000000
      }
    }
    else {
      if (metric == "substitutions") {
        val t0 = System.nanoTime()
        distances = pairs.map(x => DDD.computeSubsDist(x)).toMap
        val t1 = System.nanoTime()
        dist_time=(t1 - t0)/1000000
      }
      if (metric == "p") {
        val t0 = System.nanoTime()
        distances = pairs.map(x => DDD.computePDist(x)).toMap
        val t1 = System.nanoTime()
        dist_time=(t1 - t0)/1000000
      }
    }

    if (!par_joining) {
      val neighbourJoining = new NeighbourJoining()
      neighbourJoining.init(distances)
      val t0 = System.nanoTime()
      neighbourJoining.NJ()
      val t1 = System.nanoTime()
      nj_time=(t1 - t0)/1000000
      graph=neighbourJoining.graph
    }

    else {
      val parNeighbourJoining = new ParNeighbourJoining(sc)
      val t0 = System.nanoTime()
      parNeighbourJoining.NJ(distances)
      val t1 = System.nanoTime()
      nj_time=(t1 - t0)/1000000
      graph=parNeighbourJoining.graph.collect().toMap
    }
    ("dist_time" -> dist_time,
      "nj_time" -> nj_time,
      "graph" -> graph,
      "labels" -> data.map(x=>(x._1, x._2)))
  }
}


object main{
  def main(argv: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Phylogenetic Tree").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*val files = new java.io.File("dataset").listFiles.filter(_.getName.endsWith(".fasta")).map((x)=>"dataset/"+x.getName)

    val names = files.map((x)=> new FastaReader(x).next()._1)zip(0 until files.size)
    val sequences = files.map((x)=> new NucleotideSequence(x).read(0))

    println(files(0))
    println(names(0))
    println(sequences(0))

    var pairs : List[((Int, Option[Array[Char]]), (Int, Option[Array[Char]]))] = List[((Int, Option[Array[Char]]), (Int, Option[Array[Char]]))]()

    for (i <- 0 until sequences.size){
      for (j<- 0 until i){
        pairs = pairs :+ ((i, sequences(i)), (j, sequences(j)))
      }
    }


    // Calcolo parallelo delle distanze


    val ppairs = sc.parallelize(pairs)
    /*
        val distances = ppairs.map((x)=>((x._1._1, x._2._1), x._1._2 match {
          case None => 1.0
          case Some(s1) => x._2._2 match {
            case None => 1.0
            case Some(s2) =>
              {
                val ld = new lDistance;
                ld.pDistance (s1, s2) match{
                  case None => 1.0
                  case Some(d) => d
                }
              }
          }
        })).collect().toMap;
    */

    val distances = ppairs.map((x)=>((x._1._1, x._2._1), x._1._2 match {
      case None => 1.0
      case Some(s1) => x._2._2 match {
        case None => 1.0
        case Some(s2) =>
        {
          val ld = new lDistance;
          ld.substitutions (s1, s2)
        }
      }
    })).collect().toMap;

    //println(distances)
    val dist: Map[(Int, Int), Double] = Map((3, 2) -> 14, (3, 1) -> 18, (3, 0) -> 27, (2, 1) -> 12, (2, 0) -> 21, (1, 0) -> 17)

    val neighbourJoining = new NeighbourJoining()
    neighbourJoining.init(dist)


    neighbourJoining.NJ()
    val parneighbourJoining = new ParNeighbourJoining(sc)
    parneighbourJoining.NJ(dist)
    println(parneighbourJoining.graph.collect().toMap)
    println(neighbourJoining.graph)
    */

    val c = new Controller(true, false, "p", Seq("COVID-19_seqLunghe/alpha","COVID-19_seqLunghe/beta","COVID-19_seqLunghe/gamma"), 10, sc)
    println(c.run())
  }
}
