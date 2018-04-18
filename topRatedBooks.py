from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from subprocess import Popen, PIPE
# get book definition
def getBookDef():
	bookNames={}
	with open("books.csv") as f:
		for i,line in enumerate(f):
			if(i==0): continue
			fields=line.split(',')
			bookNames[int(fields[0])]=fields[9]
	return bookNames

def parseInput(line):
	fields=line.split(',')
	return Row(bookID=int(fields[1]),rating=int(fields[2]))


if __name__=="__main__":
	# executed directly and not called by a module
	# creating a spark session
	spark=SparkSession.builder.appName("bookRating").getOrCreate();
	# load up book id -> name dictionary
	bookNames=getBookDef()
	# load csv data to rdd
	lines=spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-books/ratings.csv")
	header = lines.first() #extract header
	lines = lines.filter(lambda row: row != header)
	books=lines.map(parseInput)
	# convert it to DataFrame
	bookDataSet=spark.createDataFrame(books)
	# compute average rating for book_id
	avgRating=bookDataSet.groupBy("bookID").avg("rating")
	# compute the count of ratings for each book id
	counts=bookDataSet.groupBy("bookID").count()
	# join the two together
	avgAndCounts=counts.join(avgRating,"bookID")
	# pull the top 10 results
	topTen=avgAndCounts.orderBy("avg(rating)",ascending=False).take(10)
	bottomTen=avgAndCounts.orderBy("avg(rating)",ascending=True).take(10)
	# print them out
	print('Top ten Best rated books')
	for book in topTen:
		print(bookNames[book[0]],book[1],book[2])
	print('Top ten Worst rated books')
	for book in bottomTen:
		print(bookNames[book[0]],book[1],book[2])
	op.close()
	# stop the session
	spark.stop()




