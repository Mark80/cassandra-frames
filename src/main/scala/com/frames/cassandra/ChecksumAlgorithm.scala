package com.frames.cassandra

import java.security.MessageDigest

trait ChecksumAlgorithm[T] {

  def execute(input: T): String

}

object ChecksumAlgorithm {

  implicit val stringM5Algorithm: ChecksumAlgorithm[String] =
    (input: String) => MessageDigest.getInstance("MD5").digest(input.getBytes).mkString

}

object Checksum {

  def calculate[T](input: T)(implicit algorithm: ChecksumAlgorithm[T]): String =
    algorithm.execute(input)

}
