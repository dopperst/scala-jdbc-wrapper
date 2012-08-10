package scala.sql.test.config

object ConnectionFactory {

	val driver = "com.mysql.jdbc.Driver"
	val url = "jdbc:mysql://at-s-aspr01/store_dev?characterEncoding=UTF-8"
	val login = "store"
	val password = "store"

	import java.sql.{DriverManager, Connection, SQLException}

	private var driverLoaded = false

	@throws(classOf[SQLException])
	private def loadDriver() {
		if (!driverLoaded) {
			Class.forName(driver).newInstance
			driverLoaded = true
		}
	}

	@throws(classOf[SQLException])
	def connect(): Connection = {
		this.synchronized { loadDriver }
		DriverManager.getConnection(url, login, password)
	}
}