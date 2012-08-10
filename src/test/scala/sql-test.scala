package scala.sql.test

import org.scalatest._

import scala.sql._
import scala.sql.conversions._
import scala.sql.test.config._

class SQLSpec extends FreeSpec {

	"Connection" - {

		implicit var connection: java.sql.Connection = null

		"should be accessible" in {
 			connection = ConnectionFactory.connect();
 		}

		"QueryStatement" - {

			var stmt: QueryStatement = null;

				"can be built from the String" in {
				stmt = new QueryStatement("SELECT * FROM store_categories")
			}
			"can be executed" in {
				assert(stmt.execute.isInstanceOf[Results] === true)
			}
			"can be closed" in {
				stmt.close
			}
		}

		"PreparedStatement" - {

			var stmt: PreparedStatement = null

			"can be built from the String" in {
				stmt = new PreparedStatement("SELECT * FROM store_categories WHERE type = :type")
			}
			"can bind existent parameters type of Int" in {
				stmt.set[Int](":type", 7)
			}
			"can rebind existent parameters type of Int" in {
				stmt.set[Int](":type", 1)
			}
			"can't bind non-existent parameters and throws SQLException" in {
				intercept[java.sql.SQLException] {
					stmt.set(":type1", 2)
				}
			}

			"Results" - {

				var results: Results = null;

				"could be produced from PrepapredStatement" in {
					results = stmt.execute
				}
				"contains single result set" in {
					assert(results.single.getOrElse(null) != null)
				}
			}

			"can be closed" in {
				stmt.close
			}
		}

		"CallableStatement" - {

			var stmt: CallableStatement = null;

			"can be built from the String" in {
				stmt = new CallableStatement("{CALL load_offers(:type, :size)}")
			}
			"can bind existent parameters type of Int" in {
				stmt.set[Int](":type", 3)
			}
			"can rebind existent parameters type of Int" in {
				stmt.set[Int](":type", 1)
			}
			"can't bind non-existent parameters and throws SQLException" in {
				intercept[java.sql.SQLException] {
					stmt.set[Int](":type1", 2)
				}
			}
			"can bind existent out parameters" in {
				stmt.out[Int](":size")
			}
			"can't bind non-existent out parameters and throws SQLException" in {
				intercept[java.sql.SQLException] {
					stmt.out[Int](":size1")
				}
			}

			"Results" - {

				var results: CallResults = null;

				"could be produced from CallableStatement" in {
					results = stmt.execute
				}
				"contains all registered output parameters " in {
					assert(results.get[Int](":size") >= 0)
				}
				"restricts access to non-registered output parameters and throws SQLException" in {
					intercept[java.sql.SQLException] {
						results.get[Int](":size1")
					}
				}
				"contains multiple results" in {
					assert(results.multiple.toList.length >= 2)
				}
			}

			"can be closed" in {
				stmt.close
			}
		}

		"UnsafeStatement" - {

			var stmt: UnsafeStatement = null;

			"can be built from the String" in {
				stmt = new UnsafeStatement("""
					SELECT COUNT(*) + @number
					FROM @table WHERE type = :type
				""")
			}
			"can bind existent unsafe parameters type of String" in {
				stmt.unsafeParam[String]("@table", "store_categories")
			}
			"can bind existent unsafe parameters type of Int" in {
				stmt.unsafeParam[Int]("@table", 1)
			}
			"can rebind existent unsafe parameters type of String" in {
				stmt.unsafeParam[String]("@table", "store_categories")
			}
			"can't bind non-existent unsafe parameters" in {
				intercept[IllegalArgumentException] {
					stmt.unsafeParam[String]("@table1", "store_categories")
				}
			}
			"can't bind safe parameters and throws IllegalArgumentException" in {
				intercept[IllegalArgumentException] {
					stmt.unsafeParam[String](":table", "store_categories")
				}
			}
			"can be prepared and produce PreparedStatement" in {
				assert(stmt.prepared.isInstanceOf[PreparedStatement] === true)
			}
		}
		"should be closeable" in {
			connection.close
		}
	}
}