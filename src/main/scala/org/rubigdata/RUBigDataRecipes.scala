package org.rubigdata

import org.jwat.warc.{WarcConstants, WarcHeader, WarcRecord}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.jsoup.nodes.Document
import org.jsoup.Jsoup
import com.optimaize.langdetect.LanguageDetector
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.profiles.LanguageProfile
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.text.TextObjectFactory
import nl.surfsara.warcutils.WarcInputFormat
import com.optimaize.langdetect.text.TextObject
import com.optimaize.langdetect.text.CommonTextObjectFactories
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URL
import util.Try
import com.google.common.base.Optional
import com.github.jsonldjava.utils.JsonUtils
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
object eRUBigDataRecipes {

  val jdHelper = new JSONLDHelper()
  var languageProfiles : java.util.List[LanguageProfile] = null
  var languageDetector : LanguageDetector = null
  var textObjectFactory : TextObjectFactory =null

  def getOF() : TextObjectFactory = {
    if (textObjectFactory == null) {
        textObjectFactory =  CommonTextObjectFactories.forDetectingOnLargeText()
    }
    return textObjectFactory
  }

  def getLD(): LanguageDetector = {
    if (languageDetector == null) {
        languageProfiles = new LanguageProfileReader().readAll()
        languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
          .withProfiles(languageProfiles)
          .build()
    }
    return languageDetector
  }

  def main(args: Array[String]) {

    val local = false //used for testing

    val conf = new SparkConf().setAppName("RUBigDataRecipes")



    //make sure everything is Serializable
    conf.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable,org.jwat.warc.WarcRecord,org.jwat.warc.WarcHeader,org.rubigdata.Recipe,org.rubigdata.NutritionInfo,org.rubigdata.Context,org.rubigdata.Ratings,org.rubigdata.Review")

    //where run
    if (local) {
      conf.setMaster("local")
    } else {
      conf.setMaster("yarn-cluster")
    }

    val sc = new SparkContext(conf)

    //local load example warc, otherwise run on complete dataset
    //
    val warcfile = if (local) "CC-MAIN-20160205193907-00258-ip-10-236-182-209.ec2.internal.warc.gz" else "hdfs:///data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/[0-3]*/warc/*"

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
      candidates.map(c => tryExtractData(c._1,c._2,c._3)).
      filter{_ != null}.
        filter{_.complete()}.
        map(c => c.toJSON()).cache().
        coalesce(500)
    //Export to json file
    parsed.saveAsTextFile(outfile)

    sc.stop()
    println("Saved to file %s".format(outfile))
  }

  /**
    * Finds candidate html pages, that might contain an recipe. Hence are HTML and contain
    * http://schema.org/Recipe
    * somewhere in the body
    */
  def findCandidates(warcf: RDD[(LongWritable, WarcRecord)]): RDD[(String, String, WarcHeader)] = {
    warcf.
      filter{ _._2.header.warcTypeIdx == 2 /* response */ }.
      filter{ _._2.getHttpHeader != null /* Should have header */}.
      filter{_._2.getHttpHeader.contentType != null /* Should have content */}.
      filter{_._2.getHttpHeader.contentType.startsWith("text/html") /* Filter on text/html*/}.
      filter{_._2.getHttpHeader.statusCode != null /* No 404 */}.
      filter{ _._2.getHttpHeader.statusCode != 404}.
      filter( wr => wr._2.hasPayload /* Should have content */).
      map(wr => (wr._2.header.warcTargetUriStr,getContent(wr._2), wr._2.header) /*Retrieve content */).
      filter{_._2.contains("schema.org") /* Check if recipe is in there somewhere*/}.cache()

  }



  /**
    * Extracts recipe from html, when not found return null
    * See: http://schema.org/Recipe
    *
    * @return
    */
  def tryExtractData(url: String, html: String, headers: WarcHeader): Recipe = {

    val doc = Jsoup.parse(html)
    var data = extractJSONLD(doc)

    if (data == null) {
      val data2 = extractMicroData(doc)
      if (data == null || (data2 != null && data.nutritionInfo != null)) {
        data = data2
      }
    }
    if (data != null) {
      data.context = fillContext(data.context, url, doc, headers)
      return data
    }
    return null
  }

  def fillContext(context: Context, url: String, doc: Document, headers: WarcHeader): Context =
  {

    var x = Try { new URL(context.url) }.toOption
    if (x.isEmpty) {
       x = Try { new URL(url) }.toOption
       context.url = url
    }
    if (!x.isEmpty) {
       context.host = x.get.getHost()
    }

    if(headers.getHeader("Content-Language") != null) {
       context.setLang(headers.getHeader("Content-Language").value, "HTTP header")

       return context
    }
    var tags = doc.select("html[lang]")
    if (tags.size() > 0) {
       context.setLang(tags.first().attr("lang"), "HTML tag")
       return context
    }
    tags = doc.select("html[xml:lang]")
    if (tags.size() > 0) {
      context.setLang(tags.first().attr("xml:lang"), "HTML tag")
      return context
    }

    var  textObject : TextObject = getOF().forText(doc.text())
    var  lang : Optional[String] = getLD().detect(textObject)
    if (lang.isPresent()) {
      context.setLang(lang.orNull().toString(), "Language Detector")
    }

    return context
  }


  def extractJSONLD(doc: Document): Recipe = {
    val txt = doc.select("script[type=\"application/ld+json\"]")
    for (t <- txt) {
      try {
        val data = JsonUtils.fromString(t.html())
        val x = jdHelper.parse(data)
        if (x != null) {
          return x
        }
      } catch {
        case e: JsonParseException => null;
        case e: JsonMappingException => null;
      }

    }
    null
  }

  /**
    * Extracts recipe from html, if not found return
    * See: http://schema.org/Recipe
    * @param html
    * @return
    */
  def extractMicroData(doc: Document): Recipe = {
    val recipes = doc.select("*[itemtype=\"http://schema.org/Recipe\"]")
    // if found
    if (recipes.size() > 0) {
      val recipe = recipes.first()
      val ni = extractNutritionInfo(recipe)
      val ratings = extractRatings(recipe)
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
        findFirstType(recipe, "description"),
        findFirstType(recipe, "cookTime"),
        findFirstType(recipe, "prepTime"),
        findFirstType(recipe, "totalTime"),
        findFirstType(recipe, "recipeInstructions"),
        findAllTypes(recipe, "suitableForDiet"),
        ni,
        ratings
      )
    }
    null
  }


  def extractRatings(e: org.jsoup.nodes.Element) : Ratings = {
    val aggs = e.select("*[itemtype=\"http://schema.org/AggregateRating\"]")
    val rating = new Ratings()
    if (aggs.size() > 0) {
      val agg = aggs.first()
      val c1 = findFirstType(agg, "ratingCount")
      val c2 = findFirstType(agg, "reviewCount")
        rating.aggregateCount += tryToInt(c1)
        rating.aggregateCount += tryToInt(c2)


      val r = findFirstType(agg, "ratingValue")
      if (r != null) {

        rating.aggregateRating = tryToDouble(r.replace(',','.'))
      }

    }
    rating.reviews = extractReviews(e)

    if (rating.notEmpty()) {
      return rating
    }
    return null
  }

  def tryToInt(txt:String, ifNull: Int = 0):Int = {
    if (txt != null) {
      try {
        return txt.toInt
      } catch {
        case e: java.lang.NumberFormatException => null
      }
    }

    return ifNull
  }

  def tryToDouble(txt:String, ifNull: Double = 0.0):Double = {
    if (txt != null) {
      try {
        return txt.toDouble
      } catch {
          case e: java.lang.NumberFormatException => null
        }
    }
    return ifNull
  }


  def extractReviews(e: org.jsoup.nodes.Element) : Array[Review] = {
    val reviews = e.select("*[itemtype=\"http://schema.org/Review\"]")
    val list = ListBuffer[Review]()
    for (r <- reviews) {
        val review = extractReview(r)
        if (review != null) {
          list += review
        }
    }
    return list.toArray
  }

  def extractReview(e: org.jsoup.nodes.Element): Review =
  {
      val review = new Review()
      review.body = findFirstType(e, "reviewBody")
      val ratings = e.select("*[itemtype=\"http://schema.org/Rating\"]")
      if (ratings.size() > 0) {
        val rating = ratings.first()
        review.ratingValue = toDouble(findFirstType(e, "ratingValue"), 0.0)
        review.worstRating = toDouble(findFirstType(e, "worstRating"), 0.0)
        review.bestRating = toDouble(findFirstType(e, "bestRating"), 5.0)
      }
      if (review.notEmpty()) {
        return review
      }
      return null

  }

  def toDouble(input: String, ifNull: Double) : Double = {
    try {
      if (input != null) {

        return input.toDouble

      }
    } catch {
      case e: java.lang.NumberFormatException => null
    }
    return ifNull

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
    val content = new java.io.ByteArrayOutputStream()

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
