import com.rabbitmq.client.*
import org.eclipse.egit.github.core.Repository
import org.eclipse.egit.github.core.client.GitHubClient
import org.eclipse.egit.github.core.client.NoSuchPageException
import org.eclipse.egit.github.core.client.PageIterator
import org.eclipse.egit.github.core.client.RequestException
import org.eclipse.egit.github.core.service.RepositoryService
import java.io.IOException
import java.nio.charset.Charset
import java.util.HashMap
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody

/**
 * Created by Neverland on 18.01.2018.
 */

val TAG="repoMinerPrepicker: "

var numberOfJavaRepositories=0
val TASKS_QUEUE_NAME = "repositoryDownloadTasksQueue";
val ACK_QUEUE_NAME = "ackQueue";
var clientInitialLimitedRequestTime = System.currentTimeMillis()

val client = GitHubClient()

class MyArgs(parser: ArgParser) {
    val user by parser.storing("login for github authentication")
    val password by parser.storing("password for github authentication")

}

fun main(args: Array<String>) {

    println(TAG+"I'm starting now...")

    var parsedArgs: MyArgs? = null

    mainBody {
        parsedArgs = ArgParser(args).parseInto(::MyArgs)
    }

    val factory = ConnectionFactory()
    factory.host = "localhost"
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val messagingArgs = HashMap<String, Any>()
    messagingArgs.put("x-max-length", 200)
    channel.queueDeclare(TASKS_QUEUE_NAME, false, false, false, messagingArgs)

    client.setCredentials(parsedArgs!!.user, parsedArgs!!.password)

    val repositoryService = RepositoryService(client)

    val repoListIterator = repositoryService.pageAllRepositories()

    var initialIteration = 0;

   sendData(connection, repoListIterator, channel)

}

fun sendData(connection: Connection, pageIterator: PageIterator<Repository>, sendChannel: Channel){

    val dataSendResult=sendDataBeforeTrigger(pageIterator, sendChannel)

    if (dataSendResult.first){
        println(TAG+"Waiting for acknowledgment (100 picked repo's should be downloaded&stored).")
        waitForDataConsumption(connection, sendChannel, pageIterator);
        return
    }

    println(TAG+"Total number of repo's: $totalNumberOfRepos")
    println(TAG+"Number of java repo's: $numberOfJavaRepositories")

    sendChannel.basicPublish("", TASKS_QUEUE_NAME, null, "stop".toByteArray())
    sendChannel.close()

}

var pagePickerCounter =0
var totalNumberOfRepos=0

private fun sendDataBeforeTrigger(pageIterator: PageIterator<Repository>,
                                  sendChannel: Channel): Pair<Boolean, Int> {
    while (pageIterator.hasNext()) {

        println("Iteration: $pagePickerCounter, remaining rate: ${client.remainingRequests}")

        try {

            var tmp=pageIterator.next().withIndex()

            for ((index, repo) in tmp) {
                println(TAG+"Index: $index, name: ${repo.name}")
                totalNumberOfRepos++
                if ((repo.language == "java") || (repo.language == null)) {

                    sendChannel.basicPublish("", TASKS_QUEUE_NAME,
                            MessageProperties.PERSISTENT_BASIC, (repo.url + "/zipball").toByteArray())
                    numberOfJavaRepositories++;
                    if ((index+1) % 100 == 0) {
                        return Pair(true,index)
                    }
                }
            }
        } catch (e: NoSuchPageException) {
            println(TAG+"Abuse/ rate limit handler processing.")
            if ((e.cause as RequestException).status == 403) {
                val sleepDuration = clientInitialLimitedRequestTime + 1000 * 60 * 60 - System.currentTimeMillis()
                Thread.sleep(sleepDuration)
                clientInitialLimitedRequestTime = System.currentTimeMillis()
            } else throw Exception(TAG+"Connection was abandoned: " + e.message)
        }

        pagePickerCounter++
    }

    return Pair(false,0)
}


fun waitForDataConsumption(connection: Connection,
                           sendChannel: Channel,
                           pageIterator: PageIterator<Repository>): Unit {

    val responseChannel = connection.createChannel()

    responseChannel.queueDeclare(ACK_QUEUE_NAME, false, false,
            false, null)

    val consumer = object : DefaultConsumer(responseChannel) {
        @Throws(IOException::class)
        override fun handleDelivery(consumerTag: String, envelope: Envelope,
                                    properties: AMQP.BasicProperties, body: ByteArray) {

            val message = String(body, Charset.forName("UTF-8"))
            if (message == "consumed") {

                sendData(connection, pageIterator, sendChannel)
                responseChannel.close()
            }
        }
    }
    responseChannel.basicConsume(ACK_QUEUE_NAME, true, consumer)
}