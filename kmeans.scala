import scala.io.Source
import scala.annotation.tailrec

//Scala code
object KMeans{
	var xCoOrdinates = List[Int]()
	var yCoOrdinates = List[Int]()
	def main(args:Array[String]) {

	println("Hello world");
	readInput()
	}

	//Function to read the input from the console
	//@return Unit
	def readInput( ) : Unit = {
	val inputFile = "/Users/Dany/Downloads/dataset2/Q1_testkmean.txt"
	try {
  	for (line <- Source.fromFile(inputFile).getLines()) {
  		var Array(m,n) = line.split(" ").map(_.toInt)
  		xCoOrdinates ::= m
  		yCoOrdinates ::= n
  		var distance$ = distance((m, n), (67, -45))
		println(distance$)
  		//println(m,n)
  		}
  		print(xCoOrdinates.length)
  		//printList(xCoOrdinates)
  		//printArray(0,xCoOrdinates)
  		//printList(yCoOrdinates)

	} catch {
  	case ex: Exception => {
  		ex.printStackTrace()
  		println("Bummer, an exception happened.")
		}
		}
	}

	//Function to print a lsit
	def printList(args: List[_]): Unit = {
  		args.foreach(println)
	}

	//Function to print a list by index
	//@return UNit
	@tailrec def printArray(i: Int, xs: List[Int]) {
  		if (i < xs.length) {
    	println("Int #" + i + " is " + xs(i))
    	printArray(i+1, xs)
  		}
	}

	def distance (p1: (Int, Int), p2: (Int, Int)) = {
  		val (p1x, p1y) = p1
  		val (p2x, p2y) = p2
  		val dx = p1x - p2x
	  	val dy = p1y - p2y
	  	Math.sqrt(dx*dx + dy*dy)
	}

}
