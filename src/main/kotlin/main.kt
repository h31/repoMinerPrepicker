import com.rabbitmq.client.ConnectionFactory
import org.eclipse.egit.github.core.client.GitHubClient
import org.eclipse.egit.github.core.client.NoSuchPageException
import org.eclipse.egit.github.core.client.RequestException
import org.eclipse.egit.github.core.service.RepositoryService
import java.util.HashMap

/**
 * Created by Neverland on 18.01.2018.
 */

fun main(args: Array<String>) {

    println("repoMinerPrepicker: I'm starting now...")

    val QUEUE_NAME = "repositoryDownloadTasksQueue";

    val factory = ConnectionFactory()
    factory.host = "localhost"
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val args = HashMap<String, Any>()
    args.put("x-max-length", 500000)  //TODO: async 100-limit wait

    val client = GitHubClient()

    client.setCredentials("Meadwild", "4069043r") //TODO: вынести на уровень конфигурации

    var clientInitialLimitedRequestTime = System.currentTimeMillis()

    val repositoryService = RepositoryService(client)

    val repoListIterator = repositoryService.pageAllRepositories()

    var i = 0;
    var numberOfJavaRepositories=0

    while (repoListIterator.hasNext()) {

        println("Iteration: $i, remaining rate: ${client.remainingRequests}")

        try {
            for ((index, repo) in repoListIterator.next().withIndex()) {
                println("Index: $index, name: ${repo.name}, language: ${repo.language}")

                if ((repo.language=="java")||(repo.language==null)){ //Реалии
                    channel.basicPublish("", QUEUE_NAME, null, (repo.url+"/zipball").toByteArray())
                    numberOfJavaRepositories++;
                    //if (channel.queueDeclare(QUEUE_NAME, false, false, false, args).messageCount==100)
                }
            }
        } catch (e: NoSuchPageException) {
            println("Abuse/ rate limit handler processing.")
            if ((e.cause as RequestException).status == 403) {
                val sleepDuration = clientInitialLimitedRequestTime + 1000 * 60 * 60 - System.currentTimeMillis()
                Thread.sleep(sleepDuration)
                clientInitialLimitedRequestTime = System.currentTimeMillis()
            } else throw Exception("Connection was abandoned.")
        }

        i++
    }

    channel.basicPublish("", QUEUE_NAME, null, "stop".toByteArray())

    channel.close()
    connection.close()

    println("$numberOfJavaRepositories were recognized.")
}