# Spanner Async Load Generator 🚀

This is a Java-based command-line tool designed to generate configurable, asynchronous load on a Google Cloud Spanner database. It utilizes `picocli` for command-line parsing and is built to test database performance under various traffic patterns like steady, stepped, and burst QPS.

### Features

  * **Asynchronous Operations**: Leverages Spanner's async client for high throughput.
  * **Multiple Workloads**: Supports `READ`, `WRITE`, and mixed `READ_WRITE` workloads.
  * **Configurable QPS**:
      * Start with an initial QPS (`--start-qps`).
      * Gradually increase QPS over time (`--step-qps`, `--interval-seconds`).
      * Set a maximum QPS ceiling (`--end-qps`).
      * Simulate a sudden traffic spike with a dedicated **burst mode** (`--burst`).
  * **Initial Data Population**: Can pre-load the target table with a specified number of rows (`--num-load-rows`).

-----

## Building the Project

This project uses Maven. To build the executable JAR, run the following command from the project's root directory:

```bash
mvn clean install
```

This will create a JAR file in the `target/` directory (e.g., `spanner-load-generator-1.5.3.jar`).

-----

## Command-Line Arguments

The tool is configurable via the following command-line options:

| Option                 | Alias | Description                                                                                    | Default           |
| :--------------------- | :---- | :--------------------------------------------------------------------------------------------- | :---------------- |
| **`--project-id`** | `-p`  | **(Required)** Google Cloud Project ID.                                                        |                   |
| **`--instance-id`** | `-i`  | **(Required)** Spanner Instance ID.                                                            |                   |
| **`--database-id`** | `-d`  | **(Required)** Spanner Database ID.                                                            |                   |
| `--table-name`         | `-t`  | Name of the table to operate on.                                                               | `LoadTestTable`   |
| `--workload`           |       | Type of workload: `READ`, `WRITE`, `READ_WRITE`.                                               | `WRITE`           |
| `--num-load-rows`      |       | Number of rows to pre-load. Recommended for `READ` workloads.                                  | `0`               |
| **`--skip-load-phase`**|       | If present, skips the initial data loading phase.                                              | `false`           |
| `--max-async-inflight` |       | Max number of concurrent async Spanner operations in flight.                                   | `1000`            |
| **`--start-qps`** |       | Initial total operations per second (QPS).                                                     | `10`              |
| **`--end-qps`** |       | Maximum total QPS. If `--burst` is used, this is the target burst QPS. `0` means no limit.      | `0`               |
| `--interval-seconds`   |       | Interval in seconds to increase QPS in step mode.                                              | `60`              |
| **`--step-qps`** |       | Percentage to increase QPS by at each interval.                                                | `5`               |
| **`--burst`** |       | If present, enables burst mode. After 15 mins, QPS jumps to `--end-qps`.                         | `false`           |
| `--num-threads`        |       | Number of worker threads initiating async operations.                                          | `10`              |
| `--run-duration-minutes` |     | Total run duration in minutes. `0` means run indefinitely.                                     | `0`               |

-----

## Examples

### 1\. Running Step QPS with a Load Phase

This scenario pre-loads the database with 100,000 rows, then starts a `READ_WRITE` workload at 100 QPS. The QPS will increase by 10% every 90 seconds until it reaches the ceiling of 2000 QPS.

```bash
java -jar target/spanner-load-generator-1.5.3.jar \
    --project-id my-gcp-project \
    --instance-id my-spanner-instance \
    --database-id my-spanner-database \
    --table-name Users \
    --workload READ_WRITE \
    --num-load-rows 100000 \
    --start-qps 100 \
    --step-qps 10 \
    --interval-seconds 90 \
    --end-qps 2000 \
    --num-threads 16
```

  * The load phase is **active** because `--skip-load-phase` is not present.
  * `--burst` is **not used**, so the tool runs in step mode from the beginning.

### 2\. Running Step QPS without a Load Phase

This example runs a `WRITE`-only workload, assuming the table is already populated. It starts at 50 QPS and increases by 5% every 60 seconds, with no upper QPS limit.

```bash
java -jar target/spanner-load-generator-1.5.3.jar \
    --project-id my-gcp-project \
    --instance-id my-spanner-instance \
    --database-id my-spanner-database \
    --table-name Products \
    --workload WRITE \
    --skip-load-phase \
    --start-qps 50 \
    --step-qps 5 \
    --interval-seconds 60 \
    --num-threads 8
```

  * The `--skip-load-phase` flag disables the initial data load.
  * Since `--end-qps` is at its default of `0`, the QPS will increase indefinitely.

### 3\. Running a "Burst" QPS without a Load Phase

This scenario tests how the system handles a sudden traffic spike. It skips the data load, runs at a baseline of 200 QPS for 15 minutes, and then **bursts** to 5000 QPS.

```bash
java -jar target/spanner-load-generator-1.5.3.jar \
    --project-id my-gcp-project \
    --instance-id my-spanner-instance \
    --database-id my-spanner-database \
    --table-name AuditLogs \
    --workload WRITE \
    --skip-load-phase \
    --start-qps 200 \
    --end-qps 5000 \
    --burst \
    --num-threads 32 \
    --run-duration-minutes 30
```

  * The **`--burst`** flag activates burst mode.
  * **`--end-qps 5000`** is crucial here; it specifies the target QPS for the burst.
  * The test will run at 200 QPS for 15 minutes, then jump to 5000 QPS.
