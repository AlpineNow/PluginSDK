/**
  * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
  */

package com.alpine.plugin.core.datasource

import com.alpine.plugin.core.annotation.AlpineSdkApi
import com.alpine.sql.DatabaseType

/**
  * :: AlpineSdkApi ::
  */
@AlpineSdkApi
trait DBDataSource extends DataSource {
  def dbType: DatabaseType.TypeValue
}
