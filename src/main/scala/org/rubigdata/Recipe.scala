package org.rubigdata

import com.google.gson.Gson
/**
  * Created by stijnvoss on 7/4/16.
  */
@SerialVersionUID(100L)
class Recipe(u: String, i: String, n: String, ingr: List[String], ryield: String, cat: List[String], method: String, cuis: List[String], ni: NutritionInfo) extends Serializable{
  var url : String = u
  var image: String = i
  var name: String = n
  var ingredients: Array[String] = ingr.toArray
  var recipeYield: String = ryield
  var category: Array[String] = cat.toArray
  var cookingMethod: String = method
  var cuisine: Array[String] = cuis.toArray
  var nutritionInfo: NutritionInfo = ni

  def complete() : Boolean = {url != null && image != null && name != null}

  def toJSON(): String = {
    val g = new Gson()
    g.toJson(this)

  }
  override  def toString() : String = {"url: " + url +"\n"+"image: "+image+"\n"+"name: "+name }

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