

import FirstDemo.{AuthorMapper, RelateCoAuthors}
import javaCode.{AuthorKey, XmlInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object WordCount {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    logger.debug("Setting up the Job conf....")

    val configuration = new Configuration
    configuration.set("xmlinput.start", "<article")
    configuration.set("xmlinput.end", "</article>")
    val job = Job.getInstance(configuration, "RelateAuthors")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[AuthorMapper])
    job.setInputFormatClass(classOf[XmlInputFormat])
    job.setCombinerClass(classOf[RelateCoAuthors])
    job.setReducerClass(classOf[RelateCoAuthors])
    job.setOutputKeyClass(classOf[AuthorKey])

    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
  }
}