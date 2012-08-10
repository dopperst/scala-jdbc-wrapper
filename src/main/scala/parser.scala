package scala.sql.parser

import scala.collection.{ mutable, immutable, generic }

sealed abstract class Token(val value: String);

case class CToken($value: String) extends Token($value);
case class PToken($value: String) extends Token($value);
case class EToken($value: String) extends Token($value);

class FTBuilder[A <: Token](implicit factory: (FTBuilder[A]) => Token) {

	private val builder = new StringBuilder;

    def append(c: Char) = { builder.append(c); this }
    def append(st: String) = { builder.append(st); this }
    def build(): Token = { factory(this) }
}

object FTBuilder {
	implicit def builder2CToken(builder: FTBuilder[CToken]) = new CToken(builder.builder.toString);
	implicit def builder2PToken(builder: FTBuilder[PToken]) = new PToken(builder.builder.toString);
	implicit def builder2EToken(builder: FTBuilder[EToken]) = new EToken(builder.builder.toString);
}

object Parser {
	val Letter = "([a-zA-Z])".r
	val Digit = "([0-9])".r	
}

class Parser {

	type Input = () => Option[Char]

	@throws(classOf[IllegalArgumentException])
	def parse(string: String): List[Token] = {
		var index = -1;
		parse(() => {
			index = index + 1;
			if (index < string.length) Some(string(index))
			else None
		})
	}

	@throws(classOf[IllegalArgumentException])
	def parse(stream: Input): List[Token] = {
		stream() match {
			case Some(':') => parsePToken(stream, new FTBuilder[PToken].append(':'))
			case Some('@') => parsePToken(stream, new FTBuilder[PToken].append('@'))
			case Some('\\') => parseEToken(stream, new FTBuilder[EToken].append('\\'))
			case Some(x) => parseCToken(stream, new FTBuilder[CToken].append(x))
			case None => Nil
		}
	}

	@throws(classOf[IllegalArgumentException])
	private def parseEToken(stream: Input, builder: FTBuilder[EToken]) : List[Token] = {
		stream() match {
			case Some(':') => builder.append(':').build() :: parse(stream)
			case Some('\\') => builder.append('\\').build() :: parse(stream)
			case _ => throw new IllegalArgumentException
		}
	}

	@throws(classOf[IllegalArgumentException])
	private def parsePToken(stream: Input, builder: FTBuilder[PToken]) : List[Token] = {
		stream() match {
			case Some(x) =>
				if ('a' <= x && x <= 'z' || 'A' <= x && x <= 'Z' || x == '_')
					parsePToken1(stream, builder.append(x))
				else throw new IllegalArgumentException
			case _ => throw new IllegalArgumentException
		}
	}

	@throws(classOf[IllegalArgumentException])
	private def parsePToken1(stream: Input, builder: FTBuilder[PToken]) : List[Token] = {
		stream() match {
			case Some(':') => builder.build() :: parsePToken(stream, new FTBuilder[PToken].append(':'))
			case Some('@') => builder.build() :: parsePToken(stream, new FTBuilder[PToken].append('@'))
			case Some('\\') => builder.build() :: parseEToken(stream, new FTBuilder[EToken].append('\\'))
			case Some(x) =>
				if ('a' <= x && x <= 'z' || 'A' <= x && x <= 'Z' || '0' <= x && x <= '9' || x == '_')
					parsePToken1(stream, builder.append(x))
				else
					builder.build() :: parseCToken(stream, new FTBuilder[CToken].append(x))
			case None => builder.build() :: Nil
		}
	}

	private def parseCToken(stream: Input, builder: FTBuilder[CToken]) : List[Token] = {
		stream() match {
			case Some(':') => builder.build() :: parsePToken(stream, new FTBuilder[PToken].append(':'))
			case Some('@') => builder.build() :: parsePToken(stream, new FTBuilder[PToken].append('@'))
			case Some('\\') => builder.build() :: parseEToken(stream, new FTBuilder[EToken].append('\\'))
			case Some(x) => parseCToken(stream, builder.append(x))
			case None => builder.build() :: Nil
		}
	}
}

/* Mutable */
/*class ParseResutls(tokens: List[Token]) {

	var tokens: Map[String, (List[Token], String)];

	def bindUnsafe[A](name: String, value: A)(implicit converter: (A) => String): ParseResutls = {
		val str = converter(value);
		val 
	}
}*/

/* Mutable */

object UnsafeQuery {
	def apply(query: String): UnsafeQuery = {
		return new UnsafeQuery((new Parser).parse(query))
	}
}

object SafeQuery {
	def apply(query: String): SafeQuery = {
		return new SafeQuery((new Parser).parse(query))
	}
}

class UnsafeQuery private[parser](var tokens: List[Token]) {

	private val unsafe: Map[String, List[(Token, Int)]] = { tokens
			.zipWithIndex
			.filter { case (t, i) => t.isInstanceOf[PToken] && t.value(0) == '@' }
			.groupBy[String] { case (t, i) => t.value }
	}

	@throws(classOf[IllegalArgumentException])
	def bindUnsafe[A](name: String, value: A)(implicit converter: (A) => String): UnsafeQuery = {
		unsafe.get(name) match {
			case Some(list) => {
				list foreach { case (t, i) =>
					tokens = tokens.updated(i, new CToken(converter(value)))
				}
			}
			case None => throw new IllegalArgumentException("Undefined parameter: " + name)
		}
		this
	}

	@throws(classOf[IllegalStateException])
	def safe(): SafeQuery = {
		tokens foreach { t => if (t.isInstanceOf[PToken] && t.value(0) == '@') throw new IllegalStateException }
		return new SafeQuery(tokens);
	}

	@throws(classOf[IllegalStateException])
	def statement(): String = {
		tokens map { t => t match {
			case CToken(value) => value
			case EToken(value) => value
			case PToken(value) => value
			case _ => throw new IllegalStateException
		}} mkString
	}
}

class SafeQuery private[parser](var tokens: List[Token]) {

	private val safe: Map[String, List[(Token, Int)]] = { tokens
			.filter { t => t.isInstanceOf[PToken] && t.value(0) == ':' }
			.zipWithIndex
			.groupBy[String] { case (t, i) => t.value }
	}

	def indicies(name: String): Option[List[Int]] = {
		safe.get(name) match {
			case Some(list) => Some(list map { case (t, i) => i })
			case None => None
		}
	}

	@throws(classOf[IllegalStateException])
	def statement(): String = {
		tokens map { t => t match {
			case CToken(value) => value
			case EToken(value) => value
			case PToken(value) => "?"
			case _ => throw new IllegalStateException
		}} mkString
	}
}