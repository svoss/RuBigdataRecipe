package org.rubigdata

import com.google.gson.Gson

import scala.collection.mutable.ListBuffer
/**
  * Created by stijnvoss on 7/4/16.
  */
@SerialVersionUID(100L)
class Recipe(u: String, i: String, n: String, ingr: List[String], ryield: String, cat: List[String], method: String, cuis: List[String], desc: String, ct: String, pt: String, tt: String, inst: String, d : List[String], ni: NutritionInfo, rats: Ratings) extends Serializable{
  var context: Context = new Context(u, "")
  var image: String = i
  var name: String = n
  var ingredients: Array[String] = ingr.toArray
  var recipeYield: String = ryield
  var category: Array[String] = cat.toArray
  var cookingMethod: String = method
  var cuisine: Array[String] = cuis.toArray
  var nutritionInfo: NutritionInfo = ni
  var description: String = desc
  var cookTime: String = ct
  var prepTime: String = pt
  var totalTime: String = tt
  var instructions: String = inst
  var diets: Array[String] = d.toArray
  var ratings: Ratings = rats

  def complete() : Boolean = {context.url != null && name != null}

  def toJSON(): String = {
    val g = new Gson()
    g.toJson(this)

  }
  override  def toString() : String = {"url: " + context.url +"\n"+"image: "+image+"\n"+"name: "+name }

}

@SerialVersionUID(100L)
class NutritionInfo(cal: String, carbo: String, chol: String, fat: String, fiber: String, protein: String, saturated: String , sodium: String, sugar: String, trans: String, unsaturated: String ,size : String) extends Serializable{
  var calories : String = cal
  var carbohydrateContent : String = carbo
  var cholesterolContent : String = chol
  var fatContent : String = fat
  var fiberContent : String = fiber
  var proteinContent : String = protein
  var saturatedFatContent : String = saturated
  var sodiumContent : String = sodium
  var sugarContent : String = sugar
  var transFatContent : String = trans
  var unsaturatedFatContent : String = unsaturated
  var servingSize: String = size

}

@SerialVersionUID(100L)
class Context( u: String, d: String) extends Serializable {
  var fullLocale: String = null
  var language: String = null
  var languageVia: String = null
  var url: String = u
  var host: String = d

  def setLang(fl: String, via: String) : Context = {
    if (fl != null && fl.length() > 1){
      language = fl.substring(0,2)
      fullLocale = fl
      languageVia = via
    }
    return this
  }
}

@SerialVersionUID(100L)
class Ratings() {
  var aggregateCount : Int = 0
  var aggregateRating: Double = 0.0
  var reviews: Array[Review] = List[Review]().toArray

  def notEmpty() : Boolean = {aggregateCount != 0 || aggregateRating != 0.0 || reviews.size > 0 }


}

@SerialVersionUID(100L)
class Review() {
  var ratingValue: Double = 0.0
  var worstRating: Double = 0.0
  var bestRating: Double = 5.0
  var author: String = null
  var body: String = null
  var datePublished : String = null

  def notEmpty() : Boolean = {ratingValue != 0.0 || body != null }
}