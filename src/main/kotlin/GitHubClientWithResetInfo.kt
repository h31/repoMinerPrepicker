import org.eclipse.egit.github.core.client.GitHubClient
import java.net.HttpURLConnection
import java.time.Duration
import java.time.Instant
import java.util.*

class GitHubClientWithResetInfo : GitHubClient() {
    var reset: Instant? = null

    override fun updateRateLimits(request: HttpURLConnection): GitHubClient {
        super.updateRateLimits(request)
        val remaining = request.getHeaderField("X-RateLimit-Reset") //$NON-NLS-1$
        if (remaining != null && remaining.isNotEmpty())
            reset = try {
                Instant.ofEpochSecond(remaining.toLong());
            } catch (nfe: NumberFormatException) {
                null
            }
        else
            reset = null

        return this
    }

    fun sleepUntilResetTime() {
        val sleepDuration = if (reset != null) {
            println("Going to sleep until $reset")
            Duration.between(Instant.now(), reset).abs()
        } else {
            println("Going to sleep for 1 minute")
            Duration.ofMinutes(1) // TODO: Maybe 1 hour?
        }
        Thread.sleep(sleepDuration.toMillis())
    }
}