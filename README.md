# cloud_plattforms_assignment1

A data warehouse in Azure Databricks that helps decide where to place 50 new fast-charging
stations: where there are many electric cars, where the electric fleet grows fast, and where
electricity is cheap. Data from SCB (new registrations, cars in traffic) and elprisetjustnu.se
(electricity prices) moves through bronze, silver and gold, orchestrated by Azure Data Factory.

| Notebook | Layer |
|---|---|
| `01_bronze_scb` | SCB API → raw files → bronze tables |
| `02_bronze_elpris` | Electricity price API → raw files → bronze table |
| `03_silver` | Typed, filtered, deduplicated and quality-checked tables |
| `04_gold` | Star schema and the decision table `charging_station_candidates` |
| `quality_checks` | Shared helper for the checks in silver and gold |

How to run, monitor and troubleshoot the pipeline: [RUNBOOK.md](RUNBOOK.md).
