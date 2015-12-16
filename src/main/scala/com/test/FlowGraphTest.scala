package com.test

import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.ActorMaterializer
import akka.stream.ClosedShape
import akka.stream.actor.ActorSubscriber
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.WatermarkRequestStrategy
import akka.stream.scaladsl.FlowGraph
import akka.stream.scaladsl.FlowGraph.Implicits.SourceArrow
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

case class Msg(int: Int)
case class ConvertedMsg(int: Int)

class FlowActor extends ActorSubscriber with ActorLogging {

  override val requestStrategy = WatermarkRequestStrategy(5)

  override def preStart = {
    log.info("FlowActor started: " + self.path)
  }

  def receive = {
    case OnNext(msg: Msg) =>
      log.info(s"executing OnNext in FlowActor.. msg: ${msg}")
      processMsg(msg)
    case OnComplete => log.info(s"Received OnComplete in FlowActor from ${sender.path}")
    /*case msg: Msg =>
      log.info(s"Received msg in FlowActor: ${msg}  from ${sender.path}")
      val convertedMsg = processMsg(msg)
    case msg: ConvertedMsg => log.info(s"Received converted msg in FlowActor: ${msg} from ${sender.path}")*/
    case obj @ _    => log.info(s"unhandelled msg in FlowActor: ${obj}, class: ${obj.getClass.getName} from ${sender.path}")
  }

  def processMsg(msg: Msg): ConvertedMsg = {
    // Do some processing
    //..
    ConvertedMsg(msg.int * 2)
  }

}

class SubsActor extends ActorSubscriber with ActorLogging {

  override val requestStrategy = WatermarkRequestStrategy(5)

  override def preStart = {
    log.info("SubsActor started: " + self.path)
  }

  def receive = {
    case OnNext(msg: ConvertedMsg) =>
      log.info(s"executing OnNext in SubsActor.. msg: ${msg}")
    case OnComplete => log.info(s"Received OnComplete in SubsActor from ${sender.path}")
    /*case msg: Msg                  => log.info(s"Received msg in SubsActor: ${msg} from ${sender.path}")
    case msg: ConvertedMsg         => log.info(s"Received converted msg in SubsActor: ${msg} from ${sender.path}")*/
    case obj @ _    => log.info(s"unhandelled msg in SubsActor: ${obj}, class: ${obj.getClass.getName} from ${sender.path}")
  }

}

object FlowGraphTest extends App {
  implicit val system = ActorSystem("FlowGraphTest")
  implicit val materializer = ActorMaterializer()

  val subActorProps = Props(new SubsActor)
  val flowActorProps = Props(new FlowActor)

  val flowActorSink = Sink.actorSubscriber(flowActorProps)
  val subsActorSink = Sink.actorSubscriber(subActorProps)

  val g3 = RunnableGraph.fromGraph(FlowGraph.create() { implicit b =>
    import FlowGraph.Implicits._

    val input: Source[Msg, Unit] = Source(1 to 10).map { x => Msg(x) }
    // prepare graph elements
    input ~> flowActorSink -> subsActorSink
    ClosedShape
  })
  println(s"running...")
  g3.run()

}
