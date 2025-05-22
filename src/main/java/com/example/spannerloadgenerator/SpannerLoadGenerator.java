package com.example.spannerloadgenerator;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.AsyncRunner.AsyncWork; // MODIFIED: Added import
import com.google.cloud.spanner.Options.TransactionOption;
// Removed unused import for TransactionCallable as we are using AsyncWork for writes
// import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Command(
    name = "spanner-load-generator",
    mixinStandardHelpOptions = true,
    version = "Spanner Load Generator 1.5.3", // Version updated
    description = "Generates configurable asynchronous load on a Google Cloud Spanner database.")
public class SpannerLoadGenerator implements Callable<Integer> {

  private static final Logger logger = LoggerFactory.getLogger(SpannerLoadGenerator.class);

  // Enum for workload type
  enum WorkloadType {
    READ,
    WRITE,
    READ_WRITE
  }

  private static final List<String> COLUMNS_TO_READ =
      Collections.unmodifiableList(Arrays.asList("Id", "Data"));

  // --- Spanner Connection Parameters ---
  @Option(
      names = {"-p", "--project-id"},
      required = true,
      description = "Google Cloud Project ID.")
  private String projectId;

  @Option(
      names = {"-i", "--instance-id"},
      required = true,
      description = "Spanner Instance ID.")
  private String instanceId;

  @Option(
      names = {"-d", "--database-id"},
      required = true,
      description = "Spanner Database ID.")
  private String databaseId;

  @Option(
      names = {"-t", "--table-name"},
      defaultValue = "LoadTestTable",
      description = "Name of the table to operate on. Default: LoadTestTable.")
  private String tableName;

  // --- Workload Configuration ---
  @Option(
      names = {"--workload"},
      defaultValue = "WRITE",
      description = "Type of workload: READ, WRITE, READ_WRITE. Default: WRITE.")
  private WorkloadType workloadType;

  @Option(
      names = {"--num-load-rows"},
      defaultValue = "0",
      description =
          "Number of rows to pre-load. Recommended for READ or READ_WRITE workloads. Default: 0.")
  private long numLoadRows;

  @Option(
      names = {"--skip-load-phase"},
      arity = "0..1",
      defaultValue = "false",
      fallbackValue = "true",
      description = "Skip the initial load phase. Default: false.")
  private boolean skipLoadPhase;

  @Option(
      names = {"--max-async-inflight"},
      defaultValue = "1000",
      description =
          "Maximum number of concurrent asynchronous Spanner operations in flight. Default: 1000.")
  private int maxAsyncInflight;

  // --- QPS Control Parameters ---
  @Option(
      names = {"--start-qps"},
      defaultValue = "10",
      description = "Initial total operations per second (QPS). Default: 10.")
  private double startQPS;

  @Option(
      names = {"--end-qps"},
      defaultValue = "0",
      description =
          "Maximum total QPS. If burst is enabled, this is the target burst QPS. 0 means no limit."
              + " Default: 0.")
  private double endQPS;

  @Option(
      names = {"--interval-seconds"},
      defaultValue = "60",
      description = "Interval in seconds to increase QPS. Default: 60.")
  private int intervalSeconds;

  @Option(
      names = {"--step-qps"},
      defaultValue = "5",
      description = "Percentage to increase QPS by at each interval. Default: 5")
  private double stepQPS;

  @Option(
      names = {"--burst"},
      arity = "0..1",
      defaultValue = "false",
      fallbackValue = "true",
      description =
          "Enable burst mode. After 15 mins, QPS will be set to endQPS (if endQPS > 0). Default:"
              + " false.")
  private boolean burstMode;

  // --- Threading Parameters ---
  @Option(
      names = {"--num-threads"},
      defaultValue = "10",
      description = "Number of worker threads initiating async operations. Default: 10.")
  private int numThreads;

  @Option(
      names = {"--run-duration-minutes"},
      defaultValue = "0",
      description = "Total run duration in minutes. 0 means run indefinitely. Default: 0.")
  private long runDurationMinutes;

  // --- Internal State ---
  private Spanner spanner;
  private DatabaseClient dbClient;
  private RateLimiter rateLimiter;
  private ExecutorService workerPool;
  private ScheduledExecutorService controlScheduler;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicLong totalSuccessfulOperations = new AtomicLong(0);
  private final AtomicLong totalFailedOperations = new AtomicLong(0);
  private final AtomicLong successfulOpsInInterval = new AtomicLong(0);
  private final AtomicLong failedOpsInInterval = new AtomicLong(0);
  private final AtomicReference<Double> currentTargetQps;
  private ScheduledFuture<?> qpsSteppingTaskFuture = null;
  private Semaphore asyncOpPermits;

  // private static final TransactionOption[] NO_TRANSACTION_OPTIONS = new TransactionOption[0]; //
  // Not needed if using AsyncWork

  public SpannerLoadGenerator() {
    this.currentTargetQps = new AtomicReference<>(0.0);
  }

  private void ensureTableExists() {
    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    try {
      logger.info("Ensuring table {} exists...", tableName);
      dbAdminClient
          .updateDatabaseDdl(
              instanceId,
              databaseId,
              Collections.singletonList(
                  "CREATE TABLE IF NOT EXISTS "
                      + tableName
                      + " ("
                      + "Id STRING(MAX) NOT NULL,"
                      + "Data STRING(MAX),"
                      + "LastUpdated TIMESTAMP OPTIONS (allow_commit_timestamp=true)"
                      + ") PRIMARY KEY (Id)"),
              null)
          .get();
      logger.info("Table {} created successfully or already existed.", tableName);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SpannerException
          && ((SpannerException) e.getCause()).getErrorCode() == ErrorCode.ALREADY_EXISTS) {
        logger.info("Table {} already exists.", tableName);
      } else if (e.getCause() instanceof AlreadyExistsException) {
        logger.info("Table {} already exists (via AlreadyExistsException).", tableName);
      } else {
        logger.error("Failed to ensure table {} exists: {}", tableName, e.getMessage(), e);
        running.set(false);
        throw new RuntimeException("Failed to ensure table " + tableName, e);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Interrupted while ensuring table {} exists.", tableName, e);
      running.set(false);
      throw new RuntimeException("Interrupted during table creation for " + tableName, e);
    }
  }

  private void performInitialLoad() {
    if (numLoadRows <= 0) {
      logger.info("Skipping initial load phase (numLoadRows is {}).", numLoadRows);
      return;
    }

    logger.info("Starting initial load phase for {} rows into table {}...", numLoadRows, tableName);
    long batchSize = 1000;
    long rowsLoadedSuccessfully = 0;
    long rowsAttempted = 0;

    for (long i = 0; i < numLoadRows && running.get(); ) {
      List<Mutation> mutations = new ArrayList<>();
      for (long j = 0; j < batchSize && (i + j) < numLoadRows; j++) {
        String id = String.format("load-row-%d", (i + j));
        String data =
            "Initial load data for row " + id + ". Timestamp: " + System.currentTimeMillis();
        mutations.add(
            Mutation.newInsertOrUpdateBuilder(tableName)
                .set("Id")
                .to(id)
                .set("Data")
                .to(data)
                .set("LastUpdated")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }

      if (!mutations.isEmpty()) {
        rowsAttempted += mutations.size();
        try {
          dbClient.writeAtLeastOnce(mutations);
          rowsLoadedSuccessfully += mutations.size();
          i += mutations.size();
          if (rowsLoadedSuccessfully % (batchSize * 10) == 0
              || rowsLoadedSuccessfully == numLoadRows
              || i >= numLoadRows) {
            logger.info(
                "Load phase: {}/{} rows loaded successfully into {}.",
                rowsLoadedSuccessfully,
                numLoadRows,
                tableName);
          }
        } catch (SpannerException e) {
          logger.error(
              "SpannerException during load phase batch (attempting to load rows starting from"
                  + " index {}): {}",
              i,
              e.getMessage());
          logger.warn("Skipping {} rows in load phase due to error.", mutations.size());
          i += mutations.size();
        }
      }
      if (!running.get()) {
        logger.warn("Shutdown initiated during load phase. Aborting load.");
        break;
      }
    }
    logger.info(
        "Initial load phase completed for table {}. Attempted: {}, Successfully loaded: {}.",
        tableName,
        rowsAttempted,
        rowsLoadedSuccessfully);
    if (rowsLoadedSuccessfully < numLoadRows && numLoadRows > 0) {
      logger.warn(
          "Not all requested rows ({}) were loaded successfully ({}). The main run phase might"
              + " encounter issues if it expects all rows to exist.",
          numLoadRows,
          rowsLoadedSuccessfully);
    }
  }

  @Override
  public Integer call() throws Exception {
    logger.info(
        "Starting Spanner Async Load Generator (v{})",
        this.getClass().getAnnotation(Command.class).version()[0]);
    logger.info(
        "Config - Project: {}, Instance: {}, Database: {}", projectId, instanceId, databaseId);
    logger.info(
        "Config - Table: {}, Workload: {}, Pre-load Rows: {}",
        tableName,
        workloadType,
        numLoadRows);
    logger.info("Config - Max Async In-flight: {}", maxAsyncInflight);
    logger.info(
        "Config - QPS: Start={}, End={}, Step={}, Interval={}s, Burst={}",
        startQPS,
        (endQPS > 0 ? endQPS : "Unlimited"),
        stepQPS,
        intervalSeconds,
        burstMode);
    logger.info("Config - Threads: {}, Run Duration: {} min", numThreads, runDurationMinutes);

    if ((workloadType == WorkloadType.READ || workloadType == WorkloadType.READ_WRITE)
        && numLoadRows == 0) {
      logger.warn(
          "Workload involves READ operations, but --num-load-rows is 0. Reads will target"
              + " non-existent 'load-row-X' IDs and likely find no data.");
    }

    asyncOpPermits = new Semaphore(maxAsyncInflight);

    SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(projectId).enableGrpcGcpExtension().build();
    spanner = options.getService();
    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
    dbClient = spanner.getDatabaseClient(db);

    ensureTableExists();
    if (!running.get()) {
      logger.error("Could not ensure table exists. Exiting.");
      if (spanner != null) spanner.close();
      return 1;
    }

    if (skipLoadPhase == false) {
      performInitialLoad();
      if (!running.get()) {
        logger.warn("Load phase was interrupted or failed. Main run may be skipped or affected.");
      }
    } else {
      logger.info("Skipping initial load phase.");
    }

    totalSuccessfulOperations.set(0);
    totalFailedOperations.set(0);
    successfulOpsInInterval.set(0);
    failedOpsInInterval.set(0);

    if (running.get()) {
      logger.info("Starting main run phase (Workload: {})...", workloadType);
      currentTargetQps.set(startQPS);
      rateLimiter = RateLimiter.create(startQPS > 0 ? startQPS : Double.MAX_VALUE);
      logger.info("RateLimiter initialized to {} QPS for main run.", rateLimiter.getRate());

      workerPool = Executors.newFixedThreadPool(numThreads);
      controlScheduler = Executors.newScheduledThreadPool(2);

      for (int i = 0; i < numThreads; i++) {
        workerPool.submit(new AsyncOperationWorker());
      }

      scheduleQpsControl();
      scheduleStatsReporting();

      if (runDurationMinutes > 0) {
        controlScheduler.schedule(this::initiateShutdown, runDurationMinutes, TimeUnit.MINUTES);
        logger.info("Main run phase scheduled for {} minutes.", runDurationMinutes);
      }
    } else {
      logger.info("Skipping main run phase due to earlier interruption or failure.");
    }

    Runtime.getRuntime().addShutdownHook(new Thread(this::initiateShutdown));

    while (running.get()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.info("Main thread interrupted, initiating shutdown.");
        initiateShutdown();
        break;
      }
    }

    logger.info("Entering final shutdown sequence...");

    if (controlScheduler != null && !controlScheduler.isShutdown()) {
      controlScheduler.shutdown();
      try {
        if (!controlScheduler.awaitTermination(10, TimeUnit.SECONDS))
          controlScheduler.shutdownNow();
      } catch (InterruptedException e) {
        controlScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    if (workerPool != null && !workerPool.isShutdown()) {
      workerPool.shutdown();
      try {
        logger.info("Waiting for worker threads to finish submitting tasks...");
        if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
          workerPool.shutdownNow();
          if (!workerPool.awaitTermination(30, TimeUnit.SECONDS))
            logger.error("Worker pool did not terminate cleanly after interrupt.");
        } else {
          logger.info("Worker threads completed task submission.");
        }
      } catch (InterruptedException e) {
        workerPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    if (asyncOpPermits != null) {
      int outstandingPermits = maxAsyncInflight - asyncOpPermits.availablePermits();
      if (outstandingPermits > 0) {
        logger.info(
            "Waiting for up to {} outstanding async Spanner operations to complete (max 60s)...",
            outstandingPermits);
        try {
          if (asyncOpPermits.tryAcquire(maxAsyncInflight, 60, TimeUnit.SECONDS)) {
            logger.info("All outstanding async operations seem to have completed.");
            asyncOpPermits.release(maxAsyncInflight);
          } else {
            logger.warn(
                "Timeout waiting for all async operations. {} permits still outstanding.",
                maxAsyncInflight - asyncOpPermits.availablePermits());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Interrupted while waiting for async operations to complete.");
        }
      } else {
        logger.info("No outstanding async Spanner operations detected at shutdown.");
      }
    }

    if (spanner != null) spanner.close();
    logger.info("Spanner client closed. Load generator finished.");
    logger.info(
        "Final Stats (Main Run Phase): Total Successful Ops: {}, Total Failed Ops: {}",
        totalSuccessfulOperations.get(),
        totalFailedOperations.get());
    return 0;
  }

  private void scheduleQpsControl() {
    if (burstMode) {
      logger.info(
          "Main Run: Burst mode enabled. Burst to endQPS (if > 0) scheduled after 15 minutes from"
              + " startQPS ({})",
          startQPS);
      controlScheduler.schedule(
          () -> {
            if (!running.get()) return;
            double burstTargetQps;
            if (endQPS > 0) {
              burstTargetQps = endQPS;
              logger.info(
                  "MAIN RUN BURST ACTIVATED! Attempting to set QPS to endQPS: {}.", burstTargetQps);
            } else {
              logger.warn(
                  "MAIN RUN BURST: Burst mode is enabled, but endQPS ({}) is not > 0. Burst will"
                      + " not change QPS based on endQPS.",
                  endQPS);
              if (intervalSeconds > 0
                  && stepQPS > 0
                  && (qpsSteppingTaskFuture == null || qpsSteppingTaskFuture.isDone())) {
                logger.info(
                    "Main Run: Starting QPS stepping after burst attempt (no change due to endQPS)."
                        + " Interval: {}s, Step: {}.",
                    intervalSeconds,
                    stepQPS);
                if (qpsSteppingTaskFuture != null) qpsSteppingTaskFuture.cancel(false);
                qpsSteppingTaskFuture =
                    controlScheduler.scheduleAtFixedRate(
                        this::performQpsStep, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
              }
              return;
            }
            currentTargetQps.set(burstTargetQps);
            rateLimiter.setRate(burstTargetQps);
            logger.info("MAIN RUN BURST: QPS set to {}.", burstTargetQps);
            if (intervalSeconds > 0 && stepQPS > 0) {
              logger.info(
                  "Main Run: Starting QPS stepping after burst. Interval: {}s, Step: {}.",
                  intervalSeconds,
                  stepQPS);
              if (qpsSteppingTaskFuture != null) qpsSteppingTaskFuture.cancel(false);
              qpsSteppingTaskFuture =
                  controlScheduler.scheduleAtFixedRate(
                      this::performQpsStep, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
            }
          },
          15,
          TimeUnit.MINUTES);
    } else if (intervalSeconds > 0 && stepQPS > 0) {
      logger.info(
          "Main Run: QPS stepping enabled (no burst). Initial delay: {}s, Interval: {}s, Step: {}.",
          intervalSeconds,
          intervalSeconds,
          stepQPS);
      if (qpsSteppingTaskFuture != null) qpsSteppingTaskFuture.cancel(false);
      qpsSteppingTaskFuture =
          controlScheduler.scheduleAtFixedRate(
              this::performQpsStep, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }
  }

  private void performQpsStep() {
    if (!running.get()) return;
    double currentRate = currentTargetQps.get();
    if (currentRate <= 0 && startQPS <= 0 && stepQPS > 0) currentRate = 0;
    if (endQPS > 0 && currentRate >= endQPS) {
      if (currentRate == endQPS
          && qpsSteppingTaskFuture != null
          && !qpsSteppingTaskFuture.isDone()) {
        logger.info("Main Run: Reached endQPS ({}). Stopping further QPS increase.", endQPS);
        qpsSteppingTaskFuture.cancel(false);
      }
      logger.debug(
          "Main Run: At or above endQPS ({}), no further stepping. Current QPS: {}",
          endQPS,
          currentRate);
      return;
    }
    double nextRateCandidate = currentRate + (currentRate * stepQPS)/100;
    double newEffectiveRate = nextRateCandidate;
    if (endQPS > 0) newEffectiveRate = Math.min(nextRateCandidate, endQPS);
    if (newEffectiveRate < 0) newEffectiveRate = 0;
    if (newEffectiveRate > currentRate
        || (currentRate == 0 && newEffectiveRate > 0 && stepQPS > 0)) {
      currentTargetQps.set(newEffectiveRate);
      rateLimiter.setRate(newEffectiveRate);
      logger.info(
          String.format("Main Run: QPS stepped from %.2f to %.2f.", currentRate, newEffectiveRate));
      if (endQPS > 0 && newEffectiveRate >= endQPS) {
        logger.info(
            String.format(
                "Main Run: Reached or exceeded endQPS (%.2f). Current target: %.2f",
                endQPS, newEffectiveRate));
        if (qpsSteppingTaskFuture != null) qpsSteppingTaskFuture.cancel(false);
      }
    } else if (newEffectiveRate == currentRate && currentRate == endQPS && endQPS > 0) {
      logger.debug("Main Run: Already at endQPS ({}). No change in QPS.", endQPS);
    }
  }

  private void scheduleStatsReporting() {
    controlScheduler.scheduleAtFixedRate(
        () -> {
          if (!running.get()) return;
          if (workerPool == null
              || workerPool.isShutdown()
              || workerPool.isTerminated() && asyncOpPermits.availablePermits() == maxAsyncInflight)
            return;

          long sucInInterval = successfulOpsInInterval.getAndSet(0);
          long failInInterval = failedOpsInInterval.getAndSet(0);
          double currentEffectiveTargetQps = currentTargetQps.get();
          double actualQpsInInterval = (sucInInterval + failInInterval) / 5.0;

          logger.info(
              String.format(
                  "STATS (Main Run): Target QPS: %.2f, Actual QPS (last 5s): %.2f, Success Ops"
                      + " (5s): %d, Failed Ops (5s): %d, Total Success Ops: %d, Total Failed Ops:"
                      + " %d, In-Flight Async: %d",
                  currentEffectiveTargetQps,
                  actualQpsInInterval,
                  sucInInterval,
                  failInInterval,
                  totalSuccessfulOperations.get(),
                  totalFailedOperations.get(),
                  maxAsyncInflight - asyncOpPermits.availablePermits()));
        },
        5,
        5,
        TimeUnit.SECONDS);
  }

  private void initiateShutdown() {
    if (running.compareAndSet(true, false)) {
      logger.info("Shutdown initiated. Stopping new operation submissions...");
      if (qpsSteppingTaskFuture != null && !qpsSteppingTaskFuture.isDone()) {
        qpsSteppingTaskFuture.cancel(true);
      }
    }
  }

  class AsyncOperationWorker implements Runnable {
    private final Random random = new Random();

    @Override
    public void run() {
      logger.debug("AsyncOperationWorker thread {} started.", Thread.currentThread().getName());
      String idForOp = "N/A_BEFORE_OP";
      String currentOpTypeStr = "N/A";

      while (running.get()) {
        try {
          rateLimiter.acquire();
          if (Thread.currentThread().isInterrupted() || !running.get()) break;

          asyncOpPermits.acquire();
          if (Thread.currentThread().isInterrupted() || !running.get()) {
            asyncOpPermits.release();
            break;
          }

          WorkloadType currentOpDecision = workloadType;
          if (workloadType == WorkloadType.READ_WRITE) {
            currentOpDecision = random.nextDouble() < 0.8 ? WorkloadType.READ : WorkloadType.WRITE;
          }

          ApiFuture<?> future = null;

          if (currentOpDecision == WorkloadType.WRITE) {
            currentOpTypeStr = "WRITE";
            if (numLoadRows > 0) {
              long rowIndex = (numLoadRows == 1) ? 0 : random.nextInt((int) numLoadRows);
              idForOp = String.format("load-row-%d", rowIndex);
            } else {
              idForOp = UUID.randomUUID().toString();
            }
            final String data =
                String.format(
                    "Async %s data. Ts: %d. Worker: %s",
                    currentOpTypeStr, System.currentTimeMillis(), Thread.currentThread().getName());
            // final Mutation mutation =
            //     Mutation.newInsertOrUpdateBuilder(tableName)
            //         .set("Id")
            //         .to(idForOp)
            //         .set("Data")
            //         .to(data)
            //         .set("LastUpdated")
            //         .to(Value.COMMIT_TIMESTAMP)
            //         .build();

            // MODIFIED: Use AsyncWork and pass executor
            AsyncWork<Long> writeWork =
                transaction -> { // transaction is AsyncTransactionContext
                  // transaction.buffer(mutation);
                  long rowIndex = (numLoadRows == 1) ? 0 : random.nextInt((int) numLoadRows);
                  String idToUse = String.format("load-row-%d", rowIndex);
                  String sql =
                      "UPDATE "
                          + tableName
                          + " SET Data = '"
                          + data
                          + "' WHERE Id = '"
                          + idToUse
                          + "'";
                  return transaction.executeUpdateAsync(Statement.of(sql));
                };
            future = dbClient.runAsync().runAsync(writeWork, MoreExecutors.directExecutor());

          } else { // READ operation
            currentOpTypeStr = "READ";
            if (numLoadRows > 0) {
              long rowIndex = (numLoadRows == 1) ? 0 : random.nextInt((int) numLoadRows);
              idForOp = String.format("load-row-%d", rowIndex);
            } else {
              idForOp = String.format("load-row-%d", random.nextInt(1000000));
            }
            future = dbClient.singleUse().readRowAsync(tableName, Key.of(idForOp), COLUMNS_TO_READ);
          }

          final String finalIdForOp = idForOp;
          final String finalCurrentOpTypeStr = currentOpTypeStr;

          ApiFutures.addCallback(
              future,
              new com.google.api.core.ApiFutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                  successfulOpsInInterval.incrementAndGet();
                  totalSuccessfulOperations.incrementAndGet();
                  if (finalCurrentOpTypeStr.equals("READ")) {
                    Struct row = (Struct) result;
                    if (row == null) {
                      logger.trace("Async READ for ID {} found no row.", finalIdForOp);
                    } else {
                      logger.trace(
                          "Async READ for ID {} successful, data: {}",
                          finalIdForOp,
                          row.getString("Data"));
                    }
                  } else {
                    logger.trace("Async WRITE for ID {} successful.", finalIdForOp);
                  }
                  asyncOpPermits.release();
                }

                @Override
                public void onFailure(Throwable t) {
                  failedOpsInInterval.incrementAndGet();
                  totalFailedOperations.incrementAndGet();
                  logger.warn(
                      "Async {} for ID {} failed: {}",
                      finalCurrentOpTypeStr,
                      finalIdForOp,
                      t.getMessage());
                  if (t instanceof SpannerException
                      && ((SpannerException) t).getErrorCode() == ErrorCode.NOT_FOUND
                      && finalCurrentOpTypeStr.equals("READ")) {}
                  asyncOpPermits.release();
                }
              },
              MoreExecutors.directExecutor());

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn(
              "AsyncOperationWorker thread {} interrupted. Exiting loop.",
              Thread.currentThread().getName());
          if (asyncOpPermits.availablePermits() < maxAsyncInflight
              && !Thread.currentThread().isInterrupted()) {}
          break;
        } catch (Exception e) {
          failedOpsInInterval.incrementAndGet();
          totalFailedOperations.incrementAndGet();
          logger.error(
              "Unexpected error in AsyncOperationWorker thread {} before launching async op (ID"
                  + " attempt: {}, Op Type: {}): {}",
              Thread.currentThread().getName(),
              idForOp,
              currentOpTypeStr,
              e.getMessage(),
              e);
          asyncOpPermits.release();
        }
      }
      logger.debug("AsyncOperationWorker thread {} finished.", Thread.currentThread().getName());
    }
  }

  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
    System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");

    try {
      StackdriverStatsExporter.createAndRegister();
    } catch (Exception e) {
      logger.error("Failed to register StackdriverStatsExporter: {}", e.getMessage(), e);
    }

    int exitCode = new CommandLine(new SpannerLoadGenerator()).execute(args);
    System.exit(exitCode);
  }
}
