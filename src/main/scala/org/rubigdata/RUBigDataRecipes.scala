package org.rubigdata

import nl.surfsara.warcutils.WarcInputFormat
import org.jwat.warc.{WarcConstants, WarcRecord}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import java.text.SimpleDateFormat
import java.util.Calendar
object eRUBigDataRecipes {


  def main(args: Array[String]) {

    val local = false //used for testing

    val conf = new SparkConf().setAppName("RUBigDataRecipes")

    //make sure everything is Serializable
    conf.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable,org.jwat.warc.WarcRecord,org.jwat.warc.WarcHeader,org.rubigdata.Recipe,org.rubigdata.NutritionInfo")

    //where run
    if (local) {
      conf.setMaster("local")
    } else {
      conf.setMaster("yarn-cluster")
    }

    val sc = new SparkContext(conf)

    //local load example warc, otherwise run on complete dataset
    val warcfile = if (local) "lasagne.warc.gz" else "hdfs:///data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/*/warc/*.gz"

    //where to output the json string
    val format = new SimpleDateFormat("y-M-d_H-m-s")
    val outfile = "recipes_" +format.format(Calendar.getInstance().getTime())+ ".json"

    //open warc files
    val warcf = sc.newAPIHadoopFile(
      warcfile,
      classOf[WarcInputFormat],               // InputFormat
      classOf[LongWritable],                  // Key
      classOf[WarcRecord]                     // Value
    )

    //Valid candidates with Reponse, text/html that contains "http://schema.org/Recipe"
    val candidates = findCandidates(warcf)
    val parsed =
      candidates.map(c => extractMicroData(c._2)).
      filter{_ != null}.
        filter{_.complete()}.
        map(c => c.toJSON())

    //Export to json file
    parsed.saveAsTextFile(outfile)
    println("Found total of %d ".format(parsed.count()))
    println("Saved to file %s".format(outfile))
    sc.stop()
  }

  /**
    * Finds candidate html pages, that might contain an recipe. Hence are HTML and contain
    * http://schema.org/Recipe
    * somewhere in the body
    */
  def findCandidates(warcf: RDD[(LongWritable, WarcRecord)]): RDD[(String, String)] = {
    warcf.
      filter{ _._2.header.warcTypeIdx == 2 /* response */ }.
      filter{ _._2.getHttpHeader != null /* Should have header */}.
      filter{_._2.getHttpHeader.contentType != null /* Should have content */}.
      filter{_._2.getHttpHeader.contentType.startsWith("text/html") /* Filter on text/html*/}.
      filter{_._2.getHttpHeader.statusCode != null /* No 404 */}.
      filter{ _._2.getHttpHeader.statusCode != 404}.
      filter( wr => wr._2.hasPayload /* Should have content */).
      map(wr => (wr._2.header.warcTargetUriStr,getContent(wr._2)) /*Retrieve content */).
      filter{_._2.contains("http://schema.org/Recipe") /* Check if recipe is in there somewhere*/}.cache()

  }

  /**
    * Extracts recipe from html, if not found return
    * See: http://schema.org/Recipe
    * @param html
    * @return
    */
  def extractMicroData(html: String): Recipe = {

    val doc = Jsoup.parse(html)
    val recipes = doc.select("*[itemtype=\"http://schema.org/Recipe\"]")
    // if found
    if (recipes.size() > 0) {
      val recipe = recipes.first()
      val ni = extractNutritionInfo(recipe)

      //from this point all attributes taken should be in own scope
      recipe.select("*[itemscope]").remove()
      return new Recipe(
        findFirstType(recipe, "url"),
        findFirstType(recipe, "image"),
        findFirstType(recipe, "name"),
        findAllTypes(recipe, "ingredients") ++ findAllTypes(recipe,"recipeIngredient"),
        findFirstType(recipe, "recipeYield"),
        findAllTypes(recipe, "recipeCategory"),
        findFirstType(recipe, "recipeMethod"),
        findAllTypes(recipe, "recipeCuisine"),
        ni
      )
    }
    null
  }

  /**
    * Gets nutrition info from inside another element(should be with itemscope recipe)
    * See: http://schema.org/NutritionInformation
    *
    * @param e
    * @return
    */
  def extractNutritionInfo(e: org.jsoup.nodes.Element): NutritionInfo = {
    val infos = e.select("*[itemtype=\"http://schema.org/NutritionInformation\"]")
    if (infos.size() > 0) {
      val info = infos.first()
       return new NutritionInfo(
        findFirstType(info, "calories"),
        findFirstType(info, "carbohydrateContent"),
        findFirstType(info, "cholesterolContent"),
        findFirstType(info, "fatContent"),
        findFirstType(info, "fiberContent"),
        findFirstType(info, "proteinContent"),
        findFirstType(info, "saturatedFatContent"),
        findFirstType(info, "sodiumContent"),
        findFirstType(info, "sugarContent"),
        findFirstType(info, "transFatContent"),
        findFirstType(info, "unsaturatedFatContent"),
        findFirstType(info, "servingSize")
      )
    }
    null
  }

  /**
    * Gets the content of an property either from a attribute(predefined for link, a,img and meta tags or the text of the element
    * @param e
    * @return String
    */
  def contentValue(e: org.jsoup.nodes.Element): String = {
    val tagName = e.tagName().toLowerCase
    if(tagName == "link" || tagName == "a") {
      return e.attr("href")
    } else if (tagName == "img"){
      return e.attr("src")
    } else if (tagName == "meta") {
      return e.attr("content")
    } else {
      return e.text()
    }
    null
  }

  /**
    * Finds first value of item property with certain name
    * @param e
    * @param t
    * @return
    */
  def findFirstType(e: org.jsoup.nodes.Element, t: String): String = {
    val x = e.select("*[itemprop=\""+t+"\"]")
    if (x.size() > 0) {
      val y = x.first()
      return contentValue(y)
    }
    null
  }

  /**
    * Finds all  values of item property with certain name
    * @param e
    * @param t
    * @return
    */
  def findAllTypes(e: org.jsoup.nodes.Element, t: String): List[String] = {
    import scala.language.implicitConversions
    val x = e.select("*[itemprop=\""+t+"\"]")
    val it = x.iterator()
    val r = new Array[String](x.size())
    var i = 0
    if (it.hasNext) {
      while (it.hasNext) {
        r(i) = contentValue(it.next())
        i += 1
      }
    }
    r.toList
  }

  /**
    * Gets content from Warc file
    * @param record
    * @return
    */
  def getContent(record: WarcRecord):String = {
    val cLen = record.header.contentLength.toInt
    //val cStream = record.getPayload.getInputStreamComplete()
    val cStream = record.getPayload.getInputStream()
    val content = new java.io.ByteArrayOutputStream();

    val buf = new Array[Byte](cLen)

    var nRead = cStream.read(buf)
    while (nRead != -1) {
      content.write(buf, 0, nRead)
      nRead = cStream.read(buf)
    }

    cStream.close()

    content.toString("UTF-8")
  }
}
