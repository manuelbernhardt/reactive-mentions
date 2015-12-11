package actors

import actors.MentionsFetcher._
import akka.actor.{ActorLogging, Actor}
import org.joda.time.DateTime
import scala.concurrent.duration._

class MentionsFetcher extends Actor with ActorLogging {

  val scheduler = context.system.scheduler.schedule(
    initialDelay = 5.seconds,
    interval = 10.minutes,
    receiver = self,
    message = CheckMentions
  )

  override def postStop(): Unit = {
    scheduler.cancel()
  }

  def receive = {
    case CheckMentions => checkMentions
    case MentionsReceived(mentions) => storeMentions(mentions)
  }

  def checkMentions = ???

  def storeMentions(mentions: Seq[Mention]) = ???

}

object MentionsFetcher {

  case object CheckMentions
  case class Mention(id: String, created_at: DateTime, text: String, from: String, users: Seq[User])
  case class User(handle: String, id: String)
  case class MentionsReceived(mentions: Seq[Mention])

}