package com.joveox.ycsb.common

import DataType._

sealed trait Field {
  val name: String
  val `type`: DataType
}

case class BOOLEANField(override val name: String) extends Field{
  override val `type` = BOOLEAN
}
case class BYTEField(override val name: String) extends Field{
  override val `type` = BYTE
}
case class SHORTField(override val name: String) extends Field{
  override val `type` = SHORT
}
case class INTField(override val name: String) extends Field{
  override val `type` = INT
}
case class LONGField(override val name: String) extends Field{
  override val `type` = LONG
}
case class FLOATField(override val name: String) extends Field{
  override val `type` = FLOAT
}
case class DOUBLEField(override val name: String) extends Field{
  override val `type` = DOUBLE
}
case class TEXTField(override val name: String) extends Field{
  override val `type` = TEXT
}
case class BLOBField(override val name: String) extends Field{
  override val `type` = BLOB
}
case class DATEField(override val name: String) extends Field{
  override val `type` = DATE
}
case class TIMESTAMPField(override val name: String) extends Field{
  override val `type` = TIMESTAMP
}
case class LISTField(override val name: String) extends Field{
  override val `type` = LIST
}
case class MAPField(override val name: String) extends Field{
  override val `type` = MAP
}


case class Schema(
                   table: String,
                   key: TEXTField,
                   fields: List[ Field ] = List.empty
                 ) {
  val allFields: List[Field] = key :: fields
}

case class SchemaStore( db: String, schemas: List[ Schema] ){

  private val byName = schemas.groupBy( _.table ).map( kv => kv._1 -> kv._2.head )

  def get( name: String ): Schema = byName( name )
}