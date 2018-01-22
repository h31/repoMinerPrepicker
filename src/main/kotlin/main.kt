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

/**
 * Created by Neverland on 18.01.2018.
 */

var numberOfJavaRepositories=0
val DOWNLOAD_TASKS_QUEUE_NAME = "repositoryDownloadTasksQueue";
val RESPONSE_QUEUE_NAME = "responseQueue";
var clientInitialLimitedRequestTime = System.currentTimeMillis()

fun main(args: Array<String>) {

    println("repoMinerPrepicker: I'm starting now...")

    val factory = ConnectionFactory()
    factory.host = "localhost"
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val client = GitHubClient()

    client.setCredentials("---", "***") //TODO: вынести на уровень конфигурации


    val repositoryService = RepositoryService(client)

    val repoListIterator = repositoryService.pageAllRepositories()

    var i = 0;

    limitedRepositoryDataPicker(repoListIterator, i, client, connection, channel)

}

/*fun waitForRepositoryDataConsumption(repoListIterator: PageIterator<Repository>,
                                     startIteration: Int,
                                     client: GitHubClient,
                                     connection: Connection,
                                     sendChannel: Channel,
                                     clientInitialLimitedRequestTime: Long): Unit{

    val responseChannel = connection.createChannel() // 2 channels -> too much (probably)
    val responseQueue=responseChannel.queueDeclare( RESPONSE_QUEUE_NAME, false, false, false, null)


    val consumer = object : DefaultConsumer(responseChannel) {
        @Throws(IOException::class)
        override fun handleDelivery(consumerTag: String, envelope: Envelope,
                                    properties: AMQP.BasicProperties, body: ByteArray) {

            val message = String(body, Charset.forName("UTF-8"))
            if (message == "consumed") {

                limitedRepositoryDataPicker(repoListIterator, startIteration, client, connection, sendChannel)


                responseChannel.close()

            }
        }
    }
    responseChannel.basicConsume(RESPONSE_QUEUE_NAME, true, consumer)
}*/

private fun limitedRepositoryDataPicker(repoListIterator: PageIterator<Repository>,
                                        startIteration: Int,
                                        client: GitHubClient,
                                        connection: Connection,
                                        sendChannel: Channel): Unit {
    var pagePicker = startIteration //pagePicker, меняется внутри функции(!)s

    val args = HashMap<String, Any>()
    args.put("x-max-length", 100)

    var totalNumberOfRepos=0

    val sendQueue=sendChannel.queueDeclare(DOWNLOAD_TASKS_QUEUE_NAME, false, false, false, args)

    while (repoListIterator.hasNext()) {

        println("Iteration: $pagePicker, remaining rate: ${client.remainingRequests}")

        try {

            sendChannel.txSelect();

            for ((index, repo) in repoListIterator.next().withIndex()) {
                println("Index: $index, name: ${repo.name}, language: ${repo.language}")
                totalNumberOfRepos++

                if ((repo.language == "java") || (repo.language == null)) { //Реалии

                    sendChannel.basicPublish("", DOWNLOAD_TASKS_QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, (repo.url + "/zipball").toByteArray())
                    sendChannel.txCommit();

                    numberOfJavaRepositories++;


                    if(totalNumberOfRepos%100==0) {
                        println("before")
                        sendChannel.waitForConfirms();
                        println("after")
                    }
                    //if (sendQueue.messageCount == 100) {
/*



                        waitForRepositoryDataConsumption(repoListIterator, i1,
                        client, connection, sendChannel,
                        clientInitialLimitedRequestTime)

                        return
*/

                    //}
                }
            }
        } catch (e: NoSuchPageException) {
            println("Abuse/ rate limit handler processing.")
            if ((e.cause as RequestException).status == 403) {
                val sleepDuration = clientInitialLimitedRequestTime + 1000 * 60 * 60 - System.currentTimeMillis()
                Thread.sleep(sleepDuration)
                clientInitialLimitedRequestTime = System.currentTimeMillis()
            } else throw Exception("Connection was abandoned: " + e.message)
        }

        pagePicker++
    }

    sendChannel.basicPublish("", DOWNLOAD_TASKS_QUEUE_NAME, null, "stop".toByteArray())

    sendChannel.close()
    connection.close()

    println("$numberOfJavaRepositories were recognized.")
}