package org.covid82

package object registryloader {
  type CountryCode = String
  type IpRange = (BigInt, BigInt)
  type IpRecord = (IpRange, CountryCode)
  type IpRegistry = IndexedSeq[IpRecord]

  case class RipeRecord(
    registry: String,
    cc: String,
    typ: String,
    start: String,
    value: String,
    date: String,
    status: String,
    extensions: String) {

    import RipeRecord.ipToBigInt
    import scala.util.Try

    val range: IpRange = typ match {
      case "ipv4" => Try {
        val from = ipToBigInt(start)
        val until = from + value.toLong
        (from, until)
      }.getOrElse(RipeRecord.emptyRange)
      case _ => RipeRecord.emptyRange
    }
  }

  object RipeRecord {
    private val base: BigInt = BigInt(256)
    val emptyRange: IpRange = (0, 0)

    def shiftAddress: Array[Byte] => Array[Int] = _.map(b => if (b < 0) b + 256 else b)

    import java.net.InetAddress
    def ipToBigInt(ip: String): BigInt = {
      val zero = (0, BigInt(0))
      val (_, address) = shiftAddress(InetAddress.getByName(ip).getAddress).foldRight(zero) {
        case (bitValue, (bitIndex, acc)) => (bitIndex + 1, acc + (bitValue * base.pow(bitIndex)))
      }
      address
    }
  }
}