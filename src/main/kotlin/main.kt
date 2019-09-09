import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import org.eclipse.egit.github.core.Repository
import org.eclipse.egit.github.core.client.NoSuchPageException
import org.eclipse.egit.github.core.client.PageIterator
import org.eclipse.egit.github.core.client.RequestException
import org.eclipse.egit.github.core.service.RepositoryService
import java.io.IOException
import java.util.*
import java.util.logging.FileHandler
import java.util.logging.Level
import java.util.logging.Logger
import java.util.logging.SimpleFormatter

/**
 * Created by Neverland on 18.01.2018 .
 */

val TAG = "repoMinerPrepicker11: "

var numberOfJavaRepositories = 0
val TASKS_QUEUE_NAME = "repositoryDownloadTasksQueue"

val client = GitHubClientWithResetInfo()

var fileLogger: Logger = Logger.getLogger(TAG + "Log")
lateinit var fileHandler: FileHandler

class MyArgs(parser: ArgParser) {

    val logger by parser.storing("logger's system path")
    val user by parser.storing("login for github authentication")
    val password by parser.storing("password for github authentication")

}

fun main(args: Array<String>) {

    var parsedArgs: MyArgs? = null

    mainBody {
        parsedArgs = ArgParser(args).parseInto(::MyArgs)
    }

    try {

        fileHandler = FileHandler(parsedArgs!!.logger)
        fileLogger.addHandler(fileHandler)
        val formatter = SimpleFormatter()
        fileHandler.formatter = formatter

    } catch (e: IOException) {
        e.printStackTrace()
    }

    val factory = ConnectionFactory()
    factory.host = "192.168.10.2"
    factory.port = 55672
    factory.username = "aaa"
    factory.password = "aaa"
    factory.requestedHeartbeat = 0
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val messagingArgs = HashMap<String, Any>()
    messagingArgs.put("x-max-length", 200)
    channel.queueDeclare(TASKS_QUEUE_NAME, false, false, false, messagingArgs)


    client.setCredentials(parsedArgs!!.user, parsedArgs!!.password)

    val repositoryService = RepositoryService(client)

    val repoListIterator = repositoryService.pageAllRepositories()

    sendData(connection, repoListIterator, channel)

}

fun sendData(connection: Connection, pageIterator: PageIterator<Repository>, sendChannel: Channel) {

    sendDataBeforeTrigger(pageIterator, sendChannel)

    fileLogger.log(Level.INFO, "Total number of repo's: $totalNumberOfRepos")
    fileLogger.log(Level.INFO, "Number of java repo's: $numberOfJavaRepositories")

    sendChannel.close()
    connection.close()

    fileHandler.close()
}

var pagePickerCounter = 0
var totalNumberOfRepos = 0

var javaRepositoriesCounter = 0

private fun sendDataBeforeTrigger(pageIterator: PageIterator<Repository>, sendChannel: Channel) {
    while (pageIterator.hasNext()) {
        fileLogger.log(Level.INFO, "Iteration: $pagePickerCounter, remaining rate: ${client.remainingRequests}")
        try {
            if (client.remainingRequests in 0..200) {
                client.sleepUntilResetTime()
            }
            val page = pageIterator.next().withIndex()

            for ((index, repo) in page) {
                fileLogger.log(Level.INFO, "Index: $index, name: ${repo.name}")
                totalNumberOfRepos++
                if (((repo.language == "java") || (repo.language == null)) && repo.size <= 2097151) {       // Для попадания в [] byte
                    fileLogger.log(Level.INFO, "Java repository, number: $javaRepositoriesCounter")
                    javaRepositoriesCounter++

                    sendChannel.basicPublish("", TASKS_QUEUE_NAME,
                            MessageProperties.PERSISTENT_BASIC, repo.url.toByteArray())
                    numberOfJavaRepositories++                                                         // Можно объединить
                }
            }
        } catch (e: NoSuchPageException) {
            fileLogger.log(Level.INFO, "Abuse/ rate limit handler processing.")
            if ((e.cause as RequestException).status == 403) {
                client.sleepUntilResetTime()
            } else {
                val exceptionMessage = "Connection was abandoned: " + e.message
                fileLogger.log(Level.INFO, exceptionMessage)

                val connection = sendChannel.connection
                sendChannel.close()
                connection.close()

                fileHandler.close()

                throw Exception(exceptionMessage)
            }
        }

        pagePickerCounter++
    }
}

//            if (message == "consumed") {
//                fileLogger!!.log(Level.INFO,"Got acknowledgment.")
//                responseChannel.close()
//                sendData(connection, pageIterator, sendChannel)
//            }
