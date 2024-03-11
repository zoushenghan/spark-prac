package com.zoush

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * @Author: zoushenghan
  * @Date: 20224/3/11 13:12
  * @Description: 汇丰测试
  */

object TestHF {

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf()
		conf.setAppName(this.getClass.getSimpleName.replaceAll("\\$", ""))
		conf.setMaster("local[1]")
		val sc: SparkContext = new SparkContext(conf)

		//样本数据1
		val data = List(
			("ABC17969(AB)", "1", "ABC17969", 2022),
			("ABC17969(AB)", "2", "CDC52533", 2022),
			("ABC17969(AB)", "3", "DEC59161", 2023),
			("ABC17969(AB)", "4", "F43874", 2022),
			("ABC17969(AB)", "5", "MY06154", 2021),
			("ABC17969(AB)", "6", "MY4387", 2022),

			("AE686(AE)", "7", "AE686", 2023),
			("AE686(AE)", "8", "BH2740", 2021),
			("AE686(AE)", "9", "EG999", 2021),
			("AE686(AE)", "10", "AE0908", 2021),
			("AE686(AE)", "11", "QA402", 2022),
			("AE686(AE)", "12", "OM691", 2022)
		)

		//样本数据2
		val data2 = List(
			("AE686(AE)", "7", "AE686", 2022),
			("AE686(AE)", "8", "BH2740", 2021),
			("AE686(AE)", "9", "EG999", 2021),
			("AE686(AE)", "10", "AE0908", 2023),
			("AE686(AE)", "11", "QA402", 2022),
			("AE686(AE)", "12", "OA691", 2022),
			("AE686(AE)", "12", "OB691", 2022),
			("AE686(AE)", "12", "OC691", 2019),
			("AE686(AE)", "12", "OD691", 2017)
		)

		//直接在此测试第 1 题
		//test1(data)

		//直接在此测试第 2 题
		//test2(sc, data)

		//直接在此测试第 3 题
		test3(sc, data2, 7)

		sc.stop()
	}

	/**
	  * 第1题测试
	  */
	def test1(data: List[(String, String, String, Int)]): ListBuffer[(String, Int)] = {
		val buf = new ListBuffer[(String, Int)]

		for (e <- data) { //循环判断每个元素是否包含指定文本即可。
			if (e._1.contains(e._3)) {
				println(s"${e._1}, ${e._4}")
				buf += ((e._1, e._4))
			}
		}

		println("====================  test 1 over!  ====================")

		buf
	}

	/**
	  * 第2题测试
	  */
	def test2(sc: SparkContext, data: List[(String, String, String, Int)]): Unit = {
		val rdd1: RDD[(String, (String, String, Int))] = sc.makeRDD(data).map(e => (e._1, (e._2, e._3, e._4)))
		val rdd2: RDD[(String, Int)] = sc.makeRDD(test1(data)) //复用第1题的结果

		//rdd通过peerId关联，并只输出满足条件的匹配数据，并转换结构
		val rdd3: RDD[((String, Int), Int)] = rdd1.join(rdd2).filter(e => e._2._1._3 <= e._2._2)
				.map(e => {
					((e._1, e._2._1._3), 1)
				})

		val result: RDD[((String, Int), Int)] = rdd3.reduceByKey(_ + _) //根据peerId和年份汇总
		result.collect().foreach(println(_))

		println("====================  test 2 over!  ====================")
	}

	/**
	  * 第3题测试
	  */
	def test3(sc: SparkContext, data: List[(String, String, String, Int)], cnt: Int): Unit = {
		val rdd1: RDD[(String, String, String, Int)] = sc.makeRDD(data)
		val rdd2: RDD[(String, (Int, Int))] = rdd1.map(e => {
			((e._1, e._4), 1)
		}).reduceByKey(_ + _)
				.map(e => {
					(e._1._1, (e._1._2, e._2))
				})

		val rdd3: RDD[(String, Iterable[(Int, Int)])] = rdd2.groupByKey() //按照peerId分组，后续对每个peerId组内的元素处理

		rdd3.collect().foreach(e => {
			val peerId = e._1
			val iterator = e._2
			val list: List[(Int, Int)] = iterator.toList //转换为List
			val sortedList = list.sortBy(e => e._1)(Ordering.Int.reverse) //按照年份降序排序

			val buf = new ListBuffer[(Int, Int)]

			if (sortedList.head._2 >= cnt) { //如果第1个元素就符合，直接输出，无需后续处理
				println(s"${peerId}, ${e._1}")
			}
			else { //如果不满足，遍历元素
				val loop = new Breaks

				loop.breakable { //定义循环，方便后续退出循环
					for (e <- sortedList.tail) { //此处把第1个元素排除。我的逻辑跟题目里面第2份样本数据输出结果的逻辑一致。
						buf += e
						if (computeYearCount(buf) >= cnt) { //汇总缓存的数据，看是否达到参数值，如果达到，输出并退出后循环。
							for (x <- buf) {
								println(s"${peerId}, ${x._1}")
							}
							loop.break
						}
					}
				}
			}
		})

		println("====================  test 3 over!  ====================")
	}

	def computeYearCount(buf: ListBuffer[(Int, Int)]): Int = {
		var count = 0
		for (e <- buf) {
			count += e._2
		}
		count
	}
}
