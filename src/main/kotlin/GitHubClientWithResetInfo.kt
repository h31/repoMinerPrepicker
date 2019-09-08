import org.eclipse.egit.github.core.client.GitHubClient
import java.net.HttpURLConnection
import java.time.Instant
import java.util.*

class GitHubClientWithResetInfo : GitHubClient() {
    var reset: Date? = null

    override fun updateRateLimits(request: HttpURLConnection): GitHubClient {
        super.updateRateLimits(request)
        val remaining = request.getHeaderField("X-RateLimit-Reset") //$NON-NLS-1$
        if (remaining != null && remaining.isNotEmpty())
            reset = try {
                Date.from( Instant.ofEpochSecond( remaining.toLong() ) );
            } catch (nfe: NumberFormatException) {
                null
            }
        else
            reset = null

        return this
    }
}