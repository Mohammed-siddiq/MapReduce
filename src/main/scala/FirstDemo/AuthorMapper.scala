package FirstDemo
import javaCode.AuthorKey
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper

import scala.xml.XML

class AuthorMapper extends Mapper[LongWritable, Text, AuthorKey, IntWritable] {


  /**
    * parse XML
    * extract the authors
    * if authors are from UIC check if they have co authors
    * ignore if they don't have co-authors
    * If they do emit all the combinations of authors
    * suppose (a1,a2,a3) are co-authors
    * then emit (a1,a2),(a1,a3),(a2,a3),(a3,a1),(a3,a2)
    */


  def processXML(xml: String): List[String] = {
    val csProf = List("Balajee Vamanan", "John Bell", "Isabel Cruz", "Bhaskar DasGupta", "Cody Cranch", "Nasim Mobasheri", "Piotr Gmytrasiewicz", "Ugo Buy", "Peter Nelson", "Robert Sloan", "Mark Grechanik", "V. N. Venkatakrishnan", "Anastasios Sidiropoulos", "Jon Solworth", "Elena Zheleva", "Lenore Zuck", "Chris Kanich", "Ajay Kshemkalyani", "Luc Renambot", "Brian Ziebart", "Prasad Sistla", "Gonzalo Bello", "Ian Kash", "Natalie Parde", "Xinhua Zhang", "Robert Kenyon", "Bing Liu", "Jason Polakis", "Evan McCarty", "Ouri Wolfson", "Cornelia Caragea", "Barbara Di Eugenio", "Philip S. Yu", "Emanuelle Burton", "G. Elisabeta Marai", "Andrew Johnson", "William Mansky", "Dale Reed", "Brent Stephens", "Scott Reckinger", "Joe Hummel", "Shanon Reckinger", "Patrick Troy", "Tanya Berger-Wolf", "David Hayes", "Xiaorui Sun", "Jakob Eriksson", "Xingbo Wu", "John Lillis", "Mitchell Theys", "Debaleena Chattopadhyay", "Daniel J. Bernstein")
    val proceedings = XML.loadString(xml)
    val authors = (proceedings \ "author").map(author => author.text).toList
    authors filter csProf.contains
  }

  def generateAuthorMapping(authors: List[String]): List[(String, String)] = {
    for {
      a1 <- authors
      a2 <- authors
      if (!a1.equals(a2))
    } yield (a1, a2)
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, AuthorKey, IntWritable]#Context): Unit = {


    /*
    <inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
<author>Mark Grechanik</author>
<author>B. M. Mainul Hossain</author>
<author>Ugo Buy</author>
<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
<pages>174-183</pages>
<year>2013</year>
<booktitle>ICST</booktitle>
<ee>https://doi.org/10.1109/ICST.2013.19</ee>
<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
<crossref>conf/icst/2013</crossref>
<url>db/conf/icst/icst2013.html#GrechanikHB13</url>
</inproceedings>
     */


    //    val test = "    <inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Mark Grechanik</author>\n<author>B. M. Mainul Hossain</author>\n<author>Ugo Buy</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
    //    val proceedingsXML = XML.loadString(test)
    //    val authors = proceedingsXML \\ "author"

    // Process the XML and extract only the CS authors form the XML input.
    val csAuthors = processXML(value.toString)

    // Generate all pairs of authors
    val authorMappings = generateAuthorMapping(csAuthors)
    val one = new IntWritable(1)

    // Mapper writing all pairs of CS authors working on the processed paper
    for (mapping <- authorMappings) context.write(new AuthorKey(new Text(mapping._1), new Text(mapping._2)), one)


  }

  //  @Test
  //  def test(): Unit = {
  //    val csProf = List("Balajee Vamanan", "John Bell", "Isabel Cruz", "Bhaskar DasGupta", "Cody Cranch", "Nasim Mobasheri", "Piotr Gmytrasiewicz", "Ugo Buy", "Peter Nelson", "Robert Sloan", "Mark Grechanik", "V. N. Venkatakrishnan", "Anastasios Sidiropoulos", "Jon Solworth", "Elena Zheleva", "Lenore Zuck", "Chris Kanich", "Ajay Kshemkalyani", "Luc Renambot", "Brian Ziebart", "Prasad Sistla", "Gonzalo Bello", "Ian Kash", "Natalie Parde", "Xinhua Zhang", "Robert Kenyon", "Bing Liu", "Jason Polakis", "Evan McCarty", "Ouri Wolfson", "Cornelia Caragea", "Barbara Di Eugenio", "Philip S. Yu", "Emanuelle Burton", "G. Elisabeta Marai", "Andrew Johnson", "William Mansky", "Dale Reed", "Brent Stephens", "Scott Reckinger", "Joe Hummel", "Shanon Reckinger", "Patrick Troy", "Tanya Berger-Wolf", "David Hayes", "Xiaorui Sun", "Jakob Eriksson", "Xingbo Wu", "John Lillis", "Mitchell Theys", "Debaleena Chattopadhyay", "Daniel J. Bernstein")
  //    val test = "    <inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Mark Grechanik</author>\n<author>B. M. Mainul Hossain</author>\n<author>Ugo Buy</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
  //    //    val proceedingsXML = XML.loadString(test)
  //    //    val authors = (proceedingsXML \ "author").map(author => author.text).toList
  //    //    //    print(authors)
  //    //    val csAuthors = authors filter (csProf.contains)
  //    //    println(csAuthors)
  //    val csAuthors = processXML(test.trim)
  //
  //    // Generate all pairs of authors
  //    val authorMappings = generateAuthorMapping(csAuthors)
  //
  //    // Mapper writing all pairs of CS authors working on the processed paper
  //    for (mapping <- authorMappings) println(new Text(mapping._1), new Text(mapping._2))
  //
  //
  //  }

}
