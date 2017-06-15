package org.rubigdata


import scala.collection.JavaConverters._
import java.util.Map
import java.util.ArrayList
import java.util.LinkedHashMap
import scala.collection.mutable.ListBuffer
class JSONLDHelper(){

  def parse(data: Object) : Recipe = {
     if (data == null) {
       return null
     }
     if (data.getClass() != classOf[LinkedHashMap[String,Object]]) {
       return null
     }
     val rootMap = asObjectMap(data)
     val context = asString(rootMap.get("@context"))
     val t = asString(rootMap.get("@type"))
     if ((t != null && context != null ) && context.toLowerCase().startsWith("http://schema.org") && t.compareToIgnoreCase("Recipe") == 0) {
       try {
         val r = new Recipe(
           kAsString(rootMap, "url"),
           findImageUrl(rootMap),
           kAsString(rootMap, "name"),
           kAsStringList(rootMap, "ingredients") ++ kAsStringList(rootMap, "recipeIngredient"),
           kAsString(rootMap, "recipeYield"),
           kAsStringList(rootMap, "recipeCategory"),
           kAsString(rootMap, "cookingMethod"),
           kAsStringList(rootMap, "recipeCuisine"),
           kAsString(rootMap, "description"),
           kAsString(rootMap, "cookTime"),
           kAsString(rootMap, "prepTime"),
           kAsString(rootMap, "totalTime"),
           kAsString(rootMap, "recipeInstructions"),
           kAsStringList(rootMap, "suitableForDiet"),
           findNutrition(rootMap),
           findRatings(rootMap)
         )

         return r
       } catch {
         case e : java.lang.ArrayStoreException  => null;
         case e : java.lang.ClassCastException  => null;
       }

     }
    null
  }



  def asObjectMap(o: Object): Map[String,Object] = {


    o.asInstanceOf[Map[String,Object]]
  }
  def asString(o: Object): String = {
    if (o == null) {
      return null
    }
    if (o.getClass() == classOf[String]) {
      return  o.asInstanceOf[String]
    } else {
      val sl  = asStringList(o)
      if (sl != null) {

        return sl.mkString(" ")
      }
      return null
    }

  }
  def asStringList(o: Object): List[String] = {
     if (o.getClass() == classOf[String]) {
       return List[String](asString(o))
     }
     if (o.getClass() == classOf[LinkedHashMap[String,Object]]) {
       return null
     }
     val x = o.asInstanceOf[ArrayList[String]]
     return x.asScala.toList

  }

  def kAsString(map : Map[String,Object], k: String) : String =  {
    if (map.containsKey(k)) {
       val item = map.get(k)
       return asString(item)

    }
    return null
  }
  def asList(o: Object): List[Object] = {
    o.asInstanceOf[ArrayList[Object]].asScala.toList
  }
  def kAsStringList(map: Map[String,Object], k : String): List[String] = {
    if (map.containsKey(k)) {

      return asStringList(map.get(k))
    }
    return  List[String]()
  }

  def findImageUrl(map: Map[String,Object]): String =
  {
    if (map.containsKey("image")) {
      val ob = map.get("image")
      if (ob == null) {
        return null
      }
      if (ob.getClass() == classOf[String]) {
        return asString(ob)
      } else {
        val map = asObjectMap(ob)
        return kAsString(map, "url")
      }
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
        return txt.replace(",",".").toDouble
      } catch {
        case e: java.lang.NumberFormatException => null
      }
    }
    return ifNull
  }
  def asInt(o: Object, defaultValue: Int = 0): Int = {
    if (o == null) {
      return defaultValue;
    }
    if (o.getClass() == classOf[String]) {
      val x = asString(o)
      return tryToInt(x,defaultValue)
    }
    return o.asInstanceOf[Int]
  }

  def kAsInt(map: Map[String,Object],key: String, defaultValue: Int = 0) : Int = {
    if (map.containsKey(key)) {
      return asInt(map.get(key), defaultValue)
    }
    return 0
  }

  def asDouble(o: Object, defaultValue: Double = 0.0): Double = {
    if (o == null) {
      return defaultValue
    }
    if (o.getClass() == classOf[String]) {
      val x = asString(o)
      return tryToDouble(x, defaultValue)
    }
    if (o.getClass() == classOf[Integer]) {
      val x = asInt(o)
      return x.toDouble
    }
    return o.asInstanceOf[Double]
  }

  def kAsDouble(map: Map[String,Object],key: String, defaultValue: Double = 0.0) : Double = {
    if (map.containsKey(key)) {
      return asDouble(map.get(key),defaultValue)
    }
    return 0.0
  }

  def findNutrition(map: Map[String,Object]): NutritionInfo = {
      if (map.containsKey("nutrition")) {
        val  nutMap = asObjectMap(map.get("nutrition"))
        return new NutritionInfo(
          kAsString(nutMap, "calories"),
          kAsString(nutMap, "carbohydrateContent"),
          kAsString(nutMap, "cholesterolContent"),
          kAsString(nutMap, "fatContent"),
          kAsString(nutMap, "fiberContent"),
          kAsString(nutMap, "proteinContent"),
          kAsString(nutMap, "saturatedFatContent"),
          kAsString(nutMap, "sodiumContent"),
          kAsString(nutMap, "sugarContent"),
          kAsString(nutMap,"transFatContent"),
          kAsString(nutMap, "unsaturatedFatContent"),
          kAsString(nutMap, "servingSize")
        )


      }
      return null;
  }

  def findRatings(map: Map[String,Object]): Ratings = {
      val r = new Ratings()
      if (map.containsKey("aggregateRating")) {
          val  ar = asObjectMap(map.get("aggregateRating"))
          r.aggregateCount = kAsInt(ar, "reviewCount") + kAsInt(ar, "ratingCount")
          r.aggregateRating = kAsDouble(ar, "ratingValue",0.0)
      }
      val list = new ListBuffer[Review]()
      if (map.containsKey("review")) {
        val reviews = asList(map.get("review"))
        for (re <- reviews) {
          val review = convertReview(re)
          if (review != null) {
            list += review
          }

        }
      }
      r.reviews = list.toArray

      if (r.notEmpty()) {
        return r
      }
    return null
  }

  def convertReview(o: Object): Review = {
      val review = new Review()
      val reviewMap = asObjectMap(o)
      if (reviewMap.containsKey("author")) {
        if (reviewMap.get("author").getClass() == classOf[String]) {
          review.author = asString(reviewMap.get("author"))
        } else {
          val author = asObjectMap(reviewMap.get("author"))
          review.author = kAsString(author, "name")
        }

      }
      if (reviewMap.containsKey("reviewRating")) {
        val rating = asObjectMap(reviewMap.get("reviewRating"))
        review.ratingValue = kAsDouble(rating, "ratingValue", 0.0)
        review.worstRating = kAsDouble(rating, "worstRating", 0.0)
        review.bestRating = kAsDouble(rating, "bestRating", 5.0)
      }

      review.body = kAsString(reviewMap, "reviewBody")
      review.datePublished = kAsString(reviewMap, "datePublished")
      if (review.notEmpty()) {
        return review
      }
      return null

  }


}