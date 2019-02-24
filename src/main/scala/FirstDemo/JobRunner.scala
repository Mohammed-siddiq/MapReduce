

import FirstDemo.{AuthorMapper, RelateCoAuthors}
import javaCode.{AuthorKey, XmlInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object JobRunner {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val configuration = new Configuration
    configuration.set("xmlinput.start", "<article")
    configuration.set("xmlinput.end", "</article>")
    configuration.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
    val job = Job.getInstance(configuration, "RelateAuthors")
    println("Entered here...")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[AuthorMapper])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setCombinerClass(classOf[RelateCoAuthors])
    job.setReducerClass(classOf[RelateCoAuthors])
    job.setOutputKeyClass(classOf[AuthorKey])
    job.setOutputValueClass(classOf[IntWritable])
    print(args(0))
    print(args(1))
    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))
    logger.debug("Setting up the Job conf....")
    System.exit(if(job.waitForCompletion(true))  0 else 1)





  }
}