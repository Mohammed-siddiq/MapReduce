package FirstDemo

import java.lang

import javaCode.AuthorKey
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._


class RelateCoAuthors extends Reducer[AuthorKey, IntWritable, AuthorKey, IntWritable] {
  override def reduce(key: AuthorKey, values: lang.Iterable[IntWritable], context: Reducer[AuthorKey, IntWritable, AuthorKey, IntWritable]#Context): Unit = {

    var sum = values.asScala.foldLeft(0)(_ + _.get)
    context.write(key, new IntWritable(sum))
  }
}






