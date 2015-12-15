package eggman89

/**
 * Created by sneha on 12/7/2015.
 */
class hashmap  extends java.io.Serializable
{
  var obj:Map[String,Int] = Map()
  var id = 0
  def add(value:String): Int ={

    if (obj.contains(value) == true)
    {
      obj(value)
    }

    else
    {
      id = id + 1
      obj = obj +(value->id)
      id
    }
  }

  def findval(value : Int) : String = {
    val default = ("-1",0)
    obj.find(_._2==value).getOrElse(default)._1
  }
}
