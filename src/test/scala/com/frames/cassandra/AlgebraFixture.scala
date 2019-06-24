package com.frames.cassandra

trait AlgebraFixture {

  def mockAppliedScript(version: Long, fileName: String, checksum: String, success: Boolean = true, errorMessage: Option[String]) =
    AppliedScript(
      version = version,
      fileName = fileName,
      checksum = checksum,
      date = "2019-01-01",
      errorMessage = errorMessage,
      success = success,
      executionTime = 10
    )
}
