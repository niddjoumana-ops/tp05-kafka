# TP05 — Real-Time Data Streaming and Processing Using Apache Kafka

**Module:** Big Data Analytics — Second-year Master IA & Data Science (2026/2027)
**Student:** Djoumana Nid 

A simulated smart-campus sensor system: a Python producer publishes temperature/humidity events to Apache Kafka (single broker, KRaft mode, Docker Compose), and a Python consumer reads them in real time and flags temperatures of 35 °C or higher.

## Project structure

```
tp05-kafka/
├── compose.yaml
├── requirements.txt
├── producer.py
├── consumer.py
├── README.md
└── screenshots/
```

## Setup instructions

Prerequisites: Docker Desktop, Python 3.10+, Git.

### 1. Start Kafka (Task 1)

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail=50 kafka
```

The container `tp05-kafka` must be `Up`, and the logs must contain `Kafka Server started`.

### 2. Install the Python client (Task 2)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1        # Linux/macOS: source .venv/bin/activate
python -m pip install -r requirements.txt
```

### 3. Create and inspect the topic (Task 5)

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### 4. Run the consumer, then the producer (Tasks 3 and 4)

Terminal 2 (consumer, started first):

```powershell
python consumer.py
```

Terminal 1 (producer):

```powershell
python producer.py
```

The producer sends 30 JSON events (one per second). The consumer prints each measurement and prints `ALERT: High temperature detected!` when the temperature is >= 35 °C. Stop the consumer with `Ctrl+C`.

### 5. Cleanup

```powershell
docker compose down        # stop and remove the container
docker compose down -v     # also delete the stored Kafka data
```

## Results and evidence

| Evidence | File |
|---|---|
| Running container (`docker compose ps`) | `screenshots/01-compose-ps.png` |
| Topic description | `screenshots/02-topic-describe.png` |
| Producer output | `screenshots/03-producer-output.png` |
| Consumer output (with alerts) | `screenshots/04-consumer-output.png` |
| Consumer group and offsets | `screenshots/05-consumer-groups.png` |

**Topic description:** `campus-sensors` has 3 partitions and replication factor 1. For each partition, the leader is broker 1, with Replicas: 1 and Isr: 1.

**Producer/consumer run:** 30 events were produced and all 30 were consumed. 9 events had a temperature >= 35 °C and triggered an alert (including a reading of exactly 35.0 °C, which confirms the condition is `>=`).

**Consumer group offsets** (group `tp05-campus-monitor`):

| Partition | CURRENT-OFFSET | LOG-END-OFFSET | LAG |
|---|---|---|---|
| 0 | 15 | 15 | 0 |
| 1 | 9 | 9 | 0 |
| 2 | 6 | 6 | 0 |

The offsets add up to 30 (the number of events sent) and the lag is 0, so the consumer read everything. The events were spread unevenly over the partitions because the producer sends no message key, so the client distributes the messages itself; ordering is guaranteed only within a single partition.

## Answers to the report questions

**1. What problem does Apache Kafka solve in a data-streaming architecture?**
Kafka decouples the systems that produce data from the systems that consume it. It stores events durably in a distributed log, so several applications can read the same stream in real time and at their own pace, instead of waiting for static files to be written and processed.

**2. Definitions**
- **Producer:** an application that publishes events to a Kafka topic.
- **Consumer:** an application that reads events from a topic.
- **Broker:** a Kafka server that stores the data and serves producers and consumers.
- **Topic:** a named stream (category) of events.
- **Partition:** an ordered, append-only subdivision of a topic.
- **Offset:** the sequential position number of a message inside a partition.
- **Consumer group:** a set of consumers sharing the work of reading a topic; each partition is read by only one consumer of the group at a time.

**3. Difference between a topic and a partition**
A topic is the logical stream of events; partitions are its physical pieces. Partitions allow parallelism and scalability, and message order is guaranteed only inside one partition.

**4. Why `auto_offset_reset='earliest'`? When does it apply?**
It makes the consumer start from the oldest available message so that no event produced before the consumer started is missed. It applies only when the group has no committed offset (a new group ID, or expired offsets). If the group already has a committed offset, the consumer resumes from it and the setting is ignored.

**5. Purpose of the consumer group ID. What if a new one is used?**
The group ID identifies the group of consumers that share the work and that store their committed offsets in Kafka. A new group ID has no offsets, so (with `earliest`) it reads the topic from the beginning, independently of the other groups. Two groups each receive all the messages, whereas consumers within the same group share the partitions.

**6. Why is JSON suitable? One limitation**
JSON is human-readable, simple to produce and parse, and independent of the programming language. Limitation: it is verbose (larger messages) and has no enforced schema or types, unlike binary formats such as Avro or Protobuf.

**7. How does Docker Compose simplify deployment?**
One declarative file (`compose.yaml`) describes the image, ports, environment variables and volumes, and one command (`docker compose up -d`) starts the whole setup reproducibly. Starting manually would require installing Java and Kafka, writing the configuration files, and launching and wiring each service by hand.

**8. Limitations of this single-broker setup compared with a production cluster**
- No fault tolerance: if the broker stops, the whole system stops.
- Replication factor 1: no copy of the data, so data can be lost.
- Partitions give no real load distribution, since all of them live on the same broker.
- The listener is `localhost` and plain text only (no TLS, no authentication).
- Limited throughput and storage. A production cluster uses several brokers, replication factor of at least 3 and secured listeners.

**9. Evidence**
See the screenshots listed in the "Results and evidence" section.

## Problems encountered and how they were solved

1. **`python -m venv .venv` failed** (`ensurepip ... returned non-zero exit status 1`, then `OSError: [Errno 28] No space left on device`). The C: drive was almost full. I freed space by deleting large unneeded files (installers, ISO images, duplicate archives, cache), created the virtual environment without pip (`--without-pip`), then installed pip with `python -m ensurepip --upgrade`.
2. **Docker commands hung** (`docker compose ps` and `docker exec ... --create` froze) after the disk was full. I quit Docker Desktop, ran `wsl --shutdown`, restarted Docker Desktop and waited for the engine to be running; the commands then worked.
3. **`SyntaxError: unterminated string literal` in `producer.py` and `consumer.py`.** The PowerShell commands used to create the files had been pasted inside the Python files. I deleted the content and pasted only the Python code.
4. **Execution policy** blocked the activation of the virtual environment; solved with `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned`.
5. **Typo in the subject:** the topic name is `campus-sensors` (with a hyphen); the line break in the PDF had split it as `campussensors`.

## Notes

- The data are synthetic and used for educational purposes only.
- The virtual environment (`.venv/`) is not uploaded to GitHub.
