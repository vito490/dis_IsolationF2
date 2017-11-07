package progettoSisDis

trait ReturnType {

  def >(value: Double): Boolean
  def <(value: Double): Boolean
  def get(): Double
}

object ReturnType {

  private class IVal(var value: Int) extends ReturnType {
    override def >(intval: Double): Boolean = {
      return this.value > intval
    }
    override def <(intval: Double): Boolean = {
      return this.value < intval
    }
    override def get(): Double = {
      value.toDouble
    }

  }
  class DVal(var value: Double) extends ReturnType {
    def >(doubleval: Double): Boolean = {
      return this.value > doubleval
    }
    def <(doubleval: Double): Boolean = {
      return this.value < doubleval
    }
    override def get(): Double = {
      value.toDouble
    }

  }
  class SVal(var value: Short) extends ReturnType {
    override def >(shortval: Double): Boolean = {
      return this.value > shortval
    }
    override def <(shortval: Double): Boolean = {
      return this.value < shortval

    }
    override def get(): Double = {
      value.toDouble
    }

  }
  def apply(x: Any): ReturnType = {
    if (x.isInstanceOf[Int])
      return new IVal(x.asInstanceOf[Int])

    if (x.isInstanceOf[Double])
      return new DVal(x.asInstanceOf[Double])

    if (x.isInstanceOf[Short])
      return new SVal(x.asInstanceOf[Short])
    return null
  }
}
