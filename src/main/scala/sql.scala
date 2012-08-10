package scala.sql

import parser._

class Row private[sql](val rs:java.sql.ResultSet) {
	def apply[A](name: String)(implicit extractor: (java.sql.ResultSet, String) => A): A = extractor(rs, name)
	def apply[A](index: Int)(implicit extractor: (java.sql.ResultSet, Int) => A): A = extractor(rs, index)
}

class ResultSet private[sql](rs: java.sql.ResultSet) {

	private def stream(next: Boolean): Stream[Row] = {
		if (next) new Row(rs) #:: stream(rs.next())
		else Stream.Empty
	}

	def rows(): Stream[Row] = stream(rs.next())
	def close(): Unit = rs.close()
}

class Results private[sql](status: Boolean, stmt: java.sql.Statement) {

	private def stream(next: Boolean): Stream[ResultSet] = {
		if (next) new ResultSet(stmt.getResultSet) #:: stream(stmt.getMoreResults)
		else Stream.Empty
	}

	def multiple(): Stream[ResultSet] = stream(status)
	def single(): Option[ResultSet] = {
		if (status) Some(new ResultSet(stmt.getResultSet))
		else None
	}
}

class CallResults private[sql](status: Boolean, stmt: java.sql.CallableStatement, safe: SafeQuery)
	extends Results(status, stmt) {

	def get[A](name: String)(implicit getter: (java.sql.CallableStatement, Int) => A): A = {
		safe.indicies(name) match {
			case Some(list) => getter(stmt, list.head + 1)
			case None => throw new java.sql.SQLException("Undefined parameter: " + name)
		}
	}
}

class CallableStatement(statement: String)(implicit connection: java.sql.Connection) {

	private val safe: SafeQuery = SafeQuery(statement)
	private val stmt: java.sql.CallableStatement = connection.prepareCall(safe.statement)

	@throws(classOf[java.sql.SQLException])
	def set[A](name: String, value: A)(implicit setter: (java.sql.PreparedStatement, Int, A) => Unit): CallableStatement = {
		safe.indicies(name) match {
			case Some(list) => list foreach { index => setter(stmt, index + 1, value) }
			case None => throw new java.sql.SQLException("Undefined parameter: " + name)
		}
		this
	}

	def out[A](name: String)(implicit m: Manifest[A], typemap: (Manifest[A]) => Int): CallableStatement = {
		safe.indicies(name) match {
			case Some(list) => list foreach { index => stmt.registerOutParameter(index + 1, typemap(manifest[A])) }
			case None => throw new java.sql.SQLException("Undefined parameter: " + name)
		}
		this
	}

	def execute(): CallResults = new CallResults(stmt.execute(), stmt, safe)
	def close(): Unit = stmt.close
}

class PreparedStatement(statement: String)(implicit connection: java.sql.Connection) {

	private val safe: SafeQuery = SafeQuery(statement)
	private val stmt: java.sql.PreparedStatement = connection.prepareStatement(safe.statement)

	@throws(classOf[java.sql.SQLException])
	def set[A](name: String, value: A)(implicit setter: (java.sql.PreparedStatement, Int, A) => Unit): PreparedStatement = {
		safe.indicies(name) match {
			case Some(list) => list foreach { index => setter(stmt, index + 1, value) }
			case None => throw new java.sql.SQLException("Undefined parameter: " + name)
		}
		this
	}

	def execute(): Results = new Results(stmt.execute(), stmt)

	def close(): Unit = stmt.close
}

class QueryStatement(statement: String)(implicit connection: java.sql.Connection) {

	private val stmt: java.sql.Statement = connection.createStatement();

	def execute(): Results = new Results(stmt.execute(statement), stmt)
	def close(): Unit = stmt.close
}

class UnsafeStatement(statement: String)(implicit connection: java.sql.Connection) {

	private val unsafe: UnsafeQuery = UnsafeQuery(statement);

	@throws(classOf[IllegalArgumentException])
	def unsafeParam[A](name: String, value: A)(implicit converter: (A) => String): UnsafeStatement = {
		unsafe.bindUnsafe(name, converter(value))
		this
	}

	def query(): QueryStatement = new QueryStatement(unsafe.statement)
	def prepared(): PreparedStatement = new PreparedStatement(unsafe.statement)
	def callable(): CallableStatement = new CallableStatement(unsafe.statement)
}

object conversions {

	type jsqlps = java.sql.PreparedStatement

	implicit def jsqlpsSetInt(stmt: jsqlps, index: Int, value: Int): Unit = stmt.setInt(index, value)
	implicit def jsqlpsSetLong(stmt: jsqlps, index: Int, value: Long): Unit = stmt.setLong(index, value)
	implicit def jsqlpsSetString(stmt: jsqlps, index: Int, value: String): Unit = stmt.setString(index, value)
	implicit def jsqlpsSetBoolean(stmt: jsqlps, index: Int, value: Boolean): Unit = stmt.setBoolean(index, value)
	implicit def jsqlpsSetFloat(stmt: jsqlps, index: Int, value: Float): Unit = stmt.setFloat(index, value)
	implicit def jsqlpsSetDouble(stmt: jsqlps, index: Int, value: Double): Unit = stmt.setDouble(index, value)

	type jsqlcs = java.sql.CallableStatement

	implicit def jsqlrsGetInt(stmt: jsqlcs, index: Int): Int = stmt.getInt(index)
	implicit def jsqlrsGetLong(stmt: jsqlcs, index: Int): Long = stmt.getLong(index)
	implicit def jsqlrsGetString(stmt: jsqlcs, index: Int): String = stmt.getString(index)
	implicit def jsqlrsGetBoolean(stmt: jsqlcs, index: Int): Boolean = stmt.getBoolean(index)
	implicit def jsqlrsGetFloat(stmt: jsqlcs, index: Int): Float = stmt.getFloat(index)
	implicit def jsqlrsGetDouble(stmt: jsqlcs, index: Int): Double = stmt.getDouble(index)

	type jsqlrs = java.sql.ResultSet

	implicit def jsqlrsGetInt(rs: jsqlrs, name: String): Int = rs.getInt(name)
	implicit def jsqlrsGetLong(rs: jsqlrs, name: String): Long = rs.getLong(name)
	implicit def jsqlrsGetString(rs: jsqlrs, name: String): String = rs.getString(name)
	implicit def jsqlrsGetBoolean(rs: jsqlrs, name: String): Boolean = rs.getBoolean(name)
	implicit def jsqlrsGetFloat(rs: jsqlrs, name: String): Float = rs.getFloat(name)
	implicit def jsqlrsGetDouble(rs: jsqlrs, name: String): Double = rs.getDouble(name)

	implicit def jsqlrsGetInt(rs: jsqlrs, index: Int): Int = rs.getInt(index)
	implicit def jsqlrsGetLong(rs: jsqlrs, index: Int): Long = rs.getLong(index)
	implicit def jsqlrsGetString(rs: jsqlrs, index: Int): String = rs.getString(index)
	implicit def jsqlrsGetBoolean(rs: jsqlrs, index: Int): Boolean = rs.getBoolean(index)
	implicit def jsqlrsGetFloat(rs: jsqlrs, index: Int): Float = rs.getFloat(index)
	implicit def jsqlrsGetDouble(rs: jsqlrs, index: Int): Double = rs.getDouble(index)

	implicit def jsqlTypeInt(clazz: Manifest[Int]): Int = java.sql.Types.INTEGER
	implicit def jsqlTypeLong(clazz: Manifest[Long]): Int = java.sql.Types.BIGINT
	implicit def jsqlTypeString(clazz: Manifest[String]): Int = java.sql.Types.VARCHAR
	implicit def jsqlTypeBoolean(clazz: Manifest[Boolean]): Int = java.sql.Types.BOOLEAN
	implicit def jsqlTypeFloat(clazz: Manifest[Float]): Int = java.sql.Types.FLOAT
	implicit def jsqlTypeDouble(clazz: Manifest[Double]): Int = java.sql.Types.DOUBLE

	implicit def unsafeInt2String(value: Int): String = String.valueOf(value)
	implicit def unsafeLong2String(value: Long): String = String.valueOf(value)
	implicit def unsafeBoolean2String(value: Boolean): String = String.valueOf(value)
	implicit def unsafeFloat2String(value: Float): String = String.valueOf(value)
	implicit def unsafeDouble2String(value: Double): String = String.valueOf(value)
}