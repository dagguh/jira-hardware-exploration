package com.atlassian.performance.tools.hardware

import com.amazonaws.services.ec2.model.InstanceType
import com.atlassian.performance.tools.aws.api.Aws
import com.atlassian.performance.tools.aws.api.Investment
import com.atlassian.performance.tools.awsinfrastructure.api.InfrastructureFormula
import com.atlassian.performance.tools.awsinfrastructure.api.hardware.EbsEc2Instance
import com.atlassian.performance.tools.awsinfrastructure.api.jira.DataCenterFormula
import com.atlassian.performance.tools.awsinfrastructure.api.loadbalancer.ElasticLoadBalancerFormula
import com.atlassian.performance.tools.awsinfrastructure.api.storage.JiraSoftwareStorage
import com.atlassian.performance.tools.awsinfrastructure.api.virtualusers.MulticastVirtualUsersFormula
import com.atlassian.performance.tools.hardware.vu.CustomScenario
import com.atlassian.performance.tools.infrastructure.api.app.Apps
import com.atlassian.performance.tools.infrastructure.api.browser.Browser
import com.atlassian.performance.tools.infrastructure.api.browser.chromium.Chromium69
import com.atlassian.performance.tools.infrastructure.api.dataset.Dataset
import com.atlassian.performance.tools.infrastructure.api.jira.JiraLaunchTimeouts
import com.atlassian.performance.tools.infrastructure.api.jira.JiraNodeConfig
import com.atlassian.performance.tools.infrastructure.api.profiler.AsyncProfiler
import com.atlassian.performance.tools.infrastructure.api.splunk.DisabledSplunkForwarder
import com.atlassian.performance.tools.io.api.dereference
import com.atlassian.performance.tools.io.api.directories
import com.atlassian.performance.tools.jiraactions.api.*
import com.atlassian.performance.tools.jiraperformancetests.api.ProvisioningPerformanceTest
import com.atlassian.performance.tools.jirasoftwareactions.api.actions.ViewBacklogAction.Companion.VIEW_BACKLOG
import com.atlassian.performance.tools.lib.*
import com.atlassian.performance.tools.report.api.FullReport
import com.atlassian.performance.tools.report.api.StandardTimeline
import com.atlassian.performance.tools.report.api.result.CohortResult
import com.atlassian.performance.tools.report.api.result.EdibleResult
import com.atlassian.performance.tools.report.api.result.FullCohortResult
import com.atlassian.performance.tools.virtualusers.api.VirtualUserLoad
import com.atlassian.performance.tools.virtualusers.api.browsers.HeadlessChromeBrowser
import com.atlassian.performance.tools.virtualusers.api.config.VirtualUserBehavior
import com.atlassian.performance.tools.workspace.api.TaskWorkspace
import com.atlassian.performance.tools.workspace.api.TestWorkspace
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.File
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.CompletableFuture.supplyAsync

class HardwareExploration(
    private val instanceTypes: List<InstanceType>,
    private val maxNodeCount: Int,
    private val dataset: Dataset,
    private val load: VirtualUserLoad,
    private val repeats: Int,
    private val investment: Investment,
    private val aws: Aws,
    private val task: TaskWorkspace
) {

    private val browser: Browser = Chromium69()
    private val virtualUsers: VirtualUserBehavior = VirtualUserBehavior.Builder(CustomScenario::class.java)
        .load(load)
        .seed(78432)
        .diagnosticsLimit(32)
        .browser(HeadlessChromeBrowser::class.java)
        .build()
    private val awsParallelism = 8
    private val results = ConcurrentHashMap<Hardware, CompletableFuture<HardwareExplorationResult>>()
    private val logger: Logger = LogManager.getLogger(this::class.java)

    fun exploreHardware() {
        val executor = Executors.newFixedThreadPool(awsParallelism)
        try {
            exploreHardwareInParallel(executor)
        } finally {
            executor.shutdown()
            executor.awaitTermination(70, TimeUnit.MINUTES)
        }
    }

    private fun exploreHardwareInParallel(
        executor: ExecutorService
    ) {
        instanceTypes.parallelStream().forEach { instanceType ->
            for (nodeCount in 1..maxNodeCount) {
                val hardware = Hardware(instanceType, nodeCount)
                val decision = decideTesting(hardware)
                if (decision.worthExploring) {
                    results[hardware] = supplyAsync {
                        HardwareExplorationResult(
                            decision = decision,
                            testResult = getRobustResult(hardware, executor)
                        )
                    }
                } else {
                    results[hardware] = completedFuture(
                        HardwareExplorationResult(
                            decision = decision,
                            testResult = null
                        )
                    )
                }
            }
        }
        results.forEach { _, futureResult -> futureResult.get() }
        HardwareExplorationTable().summarize(
            results = results.values.map { it.get() },
            instanceTypesOrder = instanceTypes,
            table = task.isolateReport("summary.csv")
        )
    }

    private fun decideTesting(
        hardware: Hardware
    ): HardwareExplorationDecision {
        if (hardware.nodeCount < 4) {
            return HardwareExplorationDecision(
                hardware = hardware,
                worthExploring = true,
                reason = "high availability"
            )
        }
        val previousResults = results
            .filterKeys { it.instanceType == hardware.instanceType }
            .values
            .map { it.get() }
            .mapNotNull { it.testResult }
            .sortedBy { it.hardware.nodeCount }
        val apdexIncrements = previousResults
            .map { it.apdex }
            .zipWithNext { a, b -> b - a }
        val strongPositiveImpact = apdexIncrements.all { it > 0.01 }
        return if (strongPositiveImpact) {
            HardwareExplorationDecision(
                hardware = hardware,
                worthExploring = true,
                reason = "adding more nodes made enough positive impact on Apdex"
            )
        } else {
            HardwareExplorationDecision(
                hardware = hardware,
                worthExploring = false,
                reason = "adding more nodes did not improve Apdex enough"
            )
        }
    }

    private fun getRobustResult(
        hardware: Hardware,
        executor: ExecutorService
    ): HardwareTestResult {
        val reusableResults = reuseResults(hardware)
        val missingResultCount = repeats - reusableResults.size
        val freshResults = runFreshResults(hardware, missingResultCount, executor)
        val allResults = reusableResults + freshResults
        return coalesce(allResults, hardware)
    }

    private fun reuseResults(
        hardware: Hardware
    ): List<HardwareTestResult> {
        val reusableResults = listPreviousRuns(hardware).mapNotNull { reuseResult(hardware, it) }
        if (reusableResults.isNotEmpty()) {
            logger.debug("Reusing results: $reusableResults")
        }
        return reusableResults
    }

    private fun reuseResult(
        hardware: Hardware,
        previousRun: File
    ): HardwareTestResult? {
        val workspace = TestWorkspace(previousRun.toPath())
        val cohortResult = workspace.readResult(hardware.nameCohort(workspace))
        return if (cohortResult is FullCohortResult) {
            score(hardware, cohortResult, workspace)
        } else {
            null
        }
    }

    private fun listPreviousRuns(
        hardware: Hardware
    ): List<File> {
        val hardwareDirectory = hardware
            .isolateRuns(task)
            .directory
            .toFile()
        return if (hardwareDirectory.isDirectory) {
            hardwareDirectory.directories()
        } else {
            emptyList()
        }
    }

    private fun postProcess(
        rawResults: CohortResult
    ): EdibleResult {
        val timeline = StandardTimeline(load.total)
        return rawResults.prepareForJudgement(timeline)
    }

    private fun score(
        hardware: Hardware,
        results: CohortResult,
        workspace: TestWorkspace
    ): HardwareTestResult {
        val postProcessedResult = postProcess(results)
        val cohort = postProcessedResult.cohort
        if (postProcessedResult.failure != null) {
            throw Exception("$cohort failed", postProcessedResult.failure)
        }
        val labels = listOf(
            VIEW_BACKLOG,
            VIEW_BOARD,
            VIEW_ISSUE,
            VIEW_DASHBOARD,
            SEARCH_WITH_JQL,
            ADD_COMMENT_SUBMIT,
            CREATE_ISSUE_SUBMIT,
            EDIT_ISSUE_SUBMIT,
            PROJECT_SUMMARY,
            BROWSE_PROJECTS,
            BROWSE_BOARDS
        ).map { it.label }
        val metrics = postProcessedResult.actionMetrics.filter { it.label in labels }
        val hardwareResult = HardwareTestResult(
            hardware = hardware,
            apdex = Apdex().score(metrics),
            apdexSpread = 0.0,
            httpThroughput = AccessLogThroughput().gauge(workspace.digOutTheRawResults(cohort)),
            httpThroughputSpread = Throughput.ZERO,
            results = listOf(results),
            errorRate = ErrorRate().measure(metrics),
            errorRateSpread = 0.0
        )
        if (hardwareResult.errorRate > 0.05) {
            reportRaw("errors", listOf(postProcessedResult), hardware)
            throw Exception("Error rate for $cohort is too high: ${ErrorRate().measure(metrics)}")
        }
        return hardwareResult
    }

    private fun runFreshResults(
        hardware: Hardware,
        missingResultCount: Int,
        executor: ExecutorService
    ): List<HardwareTestResult> {
        if (missingResultCount <= 0) {
            return emptyList()
        }
        logger.info("Running $missingResultCount tests to get the rest of the results for $hardware")
        val nextResultNumber = chooseNextRunNumber(hardware)
        val newRuns = nextResultNumber.until(nextResultNumber + missingResultCount)
        val workspace = hardware.isolateRuns(task).directory
        return newRuns
            .map { workspace.resolve(it.toString()) }
            .map { TestWorkspace(it) }
            .map { testHardware(hardware, it, executor) }
            .map { it.get() }
    }

    private fun chooseNextRunNumber(
        hardware: Hardware
    ): Int = listPreviousRuns(hardware)
        .map { it.name }
        .mapNotNull { it.toIntOrNull() }
        .max()
        ?.plus(1)
        ?: 1

    private fun testHardware(
        hardware: Hardware,
        workspace: TestWorkspace,
        executor: ExecutorService
    ): CompletableFuture<HardwareTestResult> {
        return dataCenter(
            cohort = hardware.nameCohort(workspace),
            hardware = hardware
        ).runAsync(
            workspace,
            executor,
            virtualUsers
        ).thenApply {
            workspace.writeStatus(it)
            return@thenApply score(hardware, it, workspace)
        }
    }

    private fun dataCenter(
        cohort: String,
        hardware: Hardware
    ): ProvisioningPerformanceTest = ProvisioningPerformanceTest(
        cohort = cohort,
        infrastructureFormula = InfrastructureFormula(
            investment = investment,
            jiraFormula = DataCenterFormula(
                apps = Apps(emptyList()),
                application = JiraSoftwareStorage("7.13.0"),
                jiraHomeSource = dataset.jiraHomeSource,
                database = dataset.database,
                configs = (1..hardware.nodeCount).map {
                    JiraNodeConfig.Builder()
                        .name("jira-node-$it")
                        .profiler(AsyncProfiler())
                        .launchTimeouts(
                            JiraLaunchTimeouts.Builder()
                                .initTimeout(Duration.ofMinutes(7))
                                .build()
                        )
                        .build()
                },
                loadBalancerFormula = ElasticLoadBalancerFormula(),
                computer = EbsEc2Instance(hardware.instanceType)
            ),
            virtualUsersFormula = MulticastVirtualUsersFormula(
                nodes = 8,
                shadowJar = dereference("jpt.virtual-users.shadow-jar"),
                splunkForwarder = DisabledSplunkForwarder(),
                browser = browser
            ),
            aws = aws
        )
    )

    private fun coalesce(
        results: List<HardwareTestResult>,
        hardware: Hardware
    ): HardwareTestResult {
        val apdexes = results.map { it.apdex }
        val throughputUnit = Duration.ofSeconds(1)
        val throughputs = results
            .map { it.httpThroughput }
            .map { it.scalePeriod(throughputUnit) }
            .map { it.count }
        val errorRates = results.map { it.errorRate }
        val testResult = HardwareTestResult(
            hardware = hardware,
            apdex = apdexes.average(),
            apdexSpread = apdexes.spread(),
            httpThroughput = Throughput(throughputs.average(), throughputUnit),
            httpThroughputSpread = Throughput(throughputs.spread(), throughputUnit),
            results = results.flatMap { it.results },
            errorRate = errorRates.average(),
            errorRateSpread = errorRates.spread()
        )
        if (testResult.apdexSpread > 0.10) {
            val postProcessedResults = results.flatMap { it.results }.map { postProcess(it) }
            reportRaw("comparison", postProcessedResults, hardware)
            throw Exception("Apdex spread for $hardware is too big: ${apdexes.spread()}. Results: $results")
        }
        return testResult
    }

    private fun Iterable<Double>.spread() = max()!! - min()!!

    private fun reportRaw(
        reportName: String,
        results: List<EdibleResult>,
        hardware: Hardware
    ) {
        val workspace = hardware.isolateSubTask(task, reportName)
        FullReport().dump(
            results = results,
            workspace = TestWorkspace(workspace.directory)
        )
    }
}