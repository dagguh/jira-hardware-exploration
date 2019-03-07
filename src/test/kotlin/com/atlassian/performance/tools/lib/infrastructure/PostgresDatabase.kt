package com.atlassian.performance.tools.lib.infrastructure

import com.atlassian.performance.tools.infrastructure.api.database.Database
import com.atlassian.performance.tools.infrastructure.api.dataset.DatasetPackage
import com.atlassian.performance.tools.infrastructure.api.os.Ubuntu
import com.atlassian.performance.tools.ssh.api.SshConnection
import java.net.URI
import java.time.Duration
import java.time.Instant

class PostgresDatabase(
    private val source: DatasetPackage
) : Database {

    private val ubuntu = Ubuntu()
    private val image: DockerImage = DockerImage(
        name = "postgres:9.6.12",
        pullTimeout = Duration.ofMinutes(5)
    )

    override fun setup(ssh: SshConnection): String {
        val postgresBinaryData = source.download(ssh)
        image.run(
            ssh = ssh,
            parameters = "-p 3306:3306 -v `realpath $postgresBinaryData`:/var/lib/postgresql/data" // TODO map different ports?
        )
        return postgresBinaryData
    }

    override fun start(jira: URI, ssh: SshConnection) {
        waitForPostgres(ssh)
        replaceJiraUrl(ssh, jira)
    }

    private fun waitForPostgres(ssh: SshConnection) {
        ubuntu.install(ssh, listOf("postgresql-client"))
        val start = Instant.now()
        while (!ssh.safeExecute("mysql -h 127.0.0.1 -u root -e 'select 1;'").isSuccessful()) { // TODO poll postgres client
            if (Instant.now() > start + Duration.ofMinutes(15)) {
                throw RuntimeException("Postgres didn't start in time")
            }
            Thread.sleep(Duration.ofSeconds(10).toMillis())
        }
    }

    private fun replaceJiraUrl(ssh: SshConnection, jira: URI) {
        // TODO inject Jira URL via postgres client
        ssh.execute("""mysql -h 127.0.0.1  -u root -e "UPDATE jiradb.propertystring SET propertyvalue = '$jira' WHERE id IN (select id from jiradb.propertyentry where property_key like '%baseurl%');" """)
    }
}
