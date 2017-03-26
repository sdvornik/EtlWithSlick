package com.yahoo.sdvornik.db

import java.sql.{ResultSet, SQLException}

class ResultSetIterator[T](rs: ResultSet, converter: ResultSet => Option[T]) extends Iterator[Option[T]] {

    def hasNext: Boolean = {
        try {
            rs.isBeforeFirst || (rs.getRow != 0 && !rs.isLast)
        }
        catch  {
            case e: SQLException =>
                rethrow(e)
                false
        }
    }

    def next(): Option[T] = {
        try {
            if(rs.next()) converter(rs)
            else None
        }
        catch {
            case e: SQLException =>
                rethrow(e)
                None;
        }
    }


    def remove() {
        try {
            rs.deleteRow()
        }
        catch  {
            case e: SQLException  =>
                rethrow(e);
        }
    }

    def rethrow(e: SQLException ) {
        throw new RuntimeException(e.getMessage)
    }

}

class ResultSetIterable[T](
                            private val rs: ResultSet,
                            private val converter: ResultSet => Option[T]
                          ) extends Iterable[Option[T]] {
    val iterator: ResultSetIterator[T] = new ResultSetIterator(rs, converter)

}



