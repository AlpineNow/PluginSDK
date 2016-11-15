package com.alpine.plugin.core.icon

/**
  * Created by Jennifer Thompson on 4/27/16.
  */
case class OperatorIcon private (filePrefix: String, shapeName: String) {

  def this(filePrefix: String, shape: IconShape) {
    this(filePrefix, shape.getClass.getSimpleName.dropRight(1)) // dropRight(1) to remove the $ sign.
  }

}

object OperatorIcon {
  def apply(filePrefix: String, shape: IconShape) = new OperatorIcon(filePrefix, shape)
}

sealed trait IconShape

case object Square extends IconShape
case object Hexagon extends IconShape
case object Octagon extends IconShape
case object Diamond extends IconShape
case object Pentagon extends IconShape
case object Jewell extends IconShape
case object Triangle extends IconShape
case object UpsideDownTriangle extends IconShape
case object LeftTrapezium extends IconShape
case object RightTrapezium extends IconShape
case object UpTrapezium extends IconShape
case object DownTrapezium extends IconShape
case object StarBurst extends IconShape
case object SubFlow extends IconShape
