package com.joveox.ycsb.common

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, InputStreamReader, OutputStreamWriter, Reader, Writer}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.io.Source

object GzipUtils {

  def gzipReader( in: InputStream ): Reader = new InputStreamReader(
    new GZIPInputStream( in, 1024 * 1024 ),
    StandardCharsets.UTF_8
  )

  def gzipReader( path: Path ): Reader =
    gzipReader( Files.newInputStream( path, StandardOpenOption.READ ))

  def decompress( bytes: Array[ Byte ] ): String = {
    Source.fromInputStream(
      new GZIPInputStream(
        new ByteArrayInputStream( bytes),
        1024 * 1024
      )
    ).mkString
  }

  def gzipWriter( path: Path ): Writer = new OutputStreamWriter(
    new GZIPOutputStream( Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ), 1024 * 1024 ), StandardCharsets.UTF_8
  )

  def compress( content: String ): Array[ Byte ] = {
    compress( content.getBytes( StandardCharsets.UTF_8 ) )
  }

  def compress(input: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

}