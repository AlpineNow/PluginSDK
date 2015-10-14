// Based on EvilSql tools from Sparkling Pandas (Apache Licensed)
package org.apache.spark.sql.hive;

import org.apache.spark.sql.catalyst.expressions._

// This will break, because we do evil things. but we do them in the name of fun
object HiveSqlTools {
  def makeHiveUdaf(name: String, children: Seq[Expression]) = {
    //this doesn't work with Spark 1.5.x
    //HiveUdaf(new HiveFunctionWrapper(name), children)
    ???
  }
}
