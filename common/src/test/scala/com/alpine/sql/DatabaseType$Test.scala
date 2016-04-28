package com.alpine.sql

import org.scalatest.FunSpec

/**
  * Created by mthyen on 4/11/16.
  */
class DatabaseType$Test extends FunSpec {

    import DatabaseType._

    describe("general tests") {
        it("should handle lookup") {
            assert(postgres === DatabaseType.TypeValue("postgres"))
            //assert(postgres === DatabaseType.TypeValue("postgresql"))
        }
    }
}
