package com.wuxian.dataservice

 import scala.actors.Actor
 import scala.concurrent.ExecutionContext.Implicits.global
 import scala.concurrent.Future

object TestScalaActor extends Actor {
  override def act(): Unit = {
    loop {
      react {
        case message: String =>
          Future {
            println(s"${Thread.currentThread().getId} : Begin")
            println(message)
            Thread.sleep(5000L)
            println(s"${Thread.currentThread().getId} : Over")
          }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    TestScalaActor.start
    println("Main Begin !")
    TestScalaActor ! "Hello"
    TestScalaActor ! "World"
    TestScalaActor ! "Use"
    TestScalaActor ! "Scala"
    println("Main Over !")
  }
}