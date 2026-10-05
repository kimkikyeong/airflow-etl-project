# 여신 거래 집계 ETL 파이프라인 아키텍처

> **최종 갱신**: 2026-10-05 (Named Volume 전환, Connection 환경 변수화, DAG KST 스케줄·증분 적재·SqlSensor 반영)
> **기준 자료**: [`docker-compose.yml`](../docker-compose.yml), [`dags/loan_summary_dag.py`](../dags/loan_summary_dag.py), 기동 중인 Oracle 데이터 딕셔너리 및 Airflow Connection 조회 결과
> **적용 범위**: 로컬 개발·검증 환경 (Windows + WSL2 + Docker Desktop)
> **변경 이력**: [작업 로그 2026-10-05](worklog/2026-10-05.md)

---

## 목차

1. [시스템 개요](#1-시스템-개요)
2. [아키텍처 구조도](#2-아키텍처-구조도)
3. [기술 스택 및 인프라 명세](#3-기술-스택-및-인프라-명세)
4. [데이터 파이프라인 흐름](#4-데이터-파이프라인-흐름)
5. [데이터 스키마 정의](#5-데이터-스키마-정의)
6. [운영 고려사항](#6-운영-고려사항)

---

## 1. 시스템 개요

### 1.1 목적

여신(대출) 거래 원천 데이터(`SYSTEM.TB_LOAN_TRANSACTION`)를 **거래일자 × 상품코드** 단위로 집계해 데이터마트 테이블(`SYSTEM.DM_LOAN_TRANSACTION_SUMMARY`)에 적재합니다. 마트는 신규 실행(`NEW`)·상환(`REPAY`)·이자(`INTEREST`) 유형별 건수와 금액을 제공하며, 일별·상품별 여신 현황 분석에 사용합니다.

### 1.2 전체 흐름 요약

1. **스케줄링**: Airflow Scheduler가 매일 **01:00 KST**에 DAG `loan_transaction_summary_dag`를 실행합니다. 각 실행은 전일(KST) 하루치를 처리합니다.
2. **마트 준비·원천 대기**: 마트 테이블이 없으면 생성하고, 동시에 처리할 일자의 원천 데이터가 들어왔는지 확인합니다(최대 3시간 대기).
3. **집계·적재**: 처리 일자의 원천만 `GROUP BY` + `CASE WHEN`으로 집계하고 `MERGE INTO`로 마트에 Upsert합니다(증분 적재).
4. **실행 기록**: Task 상태와 실행 이력은 PostgreSQL 메타데이터 DB에, 실행 로그는 호스트의 `./logs` 디렉터리에 기록됩니다.
5. **데이터 보존**: Oracle 데이터와 Airflow 메타데이터는 Docker Named Volume에 저장되어 컨테이너를 삭제·재생성해도 유지됩니다.

### 1.3 설계 원칙

| 원칙 | 적용 방식 |
|---|---|
| 멱등성 (Idempotency) | `MERGE INTO` 기반 Upsert. 같은 DAG Run을 재실행해도 마트 결과가 동일함 |
| 재실행 안전성 | DDL은 `ORA-00955`(객체 이미 존재)만 무시하고 나머지 오류는 그대로 발생시킴 |
| 과거 소급 방지 | `catchup=False` 적용 |
| 표준 Operator 사용 | Deprecated된 `OracleOperator` 대신 `SQLExecuteQueryOperator` 사용 |
| 데이터 영속성 | DB 데이터는 Named Volume, 코드·로그는 호스트 디렉터리(Bind mount)에 저장 |
| 설정의 코드화 | Oracle Connection을 `.env` 환경 변수로 정의해 메타데이터 DB 상태와 무관하게 유지 |

---

## 2. 아키텍처 구조도

### 2.1 컨테이너 구성도

```mermaid
flowchart LR
    browser["사용자 브라우저"]
    sqlclient["로컬 SQL 클라이언트<br/>(DBeaver 등)"]

    subgraph HOST["Windows 11 + WSL2 · Docker Desktop"]
        hostdirs[["호스트 디렉터리<br/>./dags · ./logs · ./plugins"]]
        envfile[[".env<br/>AIRFLOW_CONN_ORACLE_DEFAULT"]]

        subgraph NET["Docker 네트워크: info-net (bridge)"]
            web["airflow-webserver<br/>Airflow 2.8.1 · :8080"]
            sch["airflow-scheduler<br/>LocalExecutor<br/>(Task 실행 주체)"]
            pg[("airflow-postgres<br/>PostgreSQL 13<br/>메타데이터 DB")]
            ora[("oracle-db<br/>Oracle Free · FREEPDB1<br/>:1521")]
        end

        pgvol[["Named Volume<br/>welcome-project_postgres_data"]]
        oravol[["Named Volume<br/>welcome-project_oracle_data"]]
    end

    browser -->|"localhost:8080"| web
    sqlclient -->|"localhost:1521"| ora
    web -->|"postgres:5432"| pg
    sch -->|"postgres:5432"| pg
    sch -->|"oracle_default<br/>oracle-db:1521 / FREEPDB1"| ora
    hostdirs -. "bind mount" .-> web
    hostdirs -. "bind mount" .-> sch
    envfile -. "환경 변수 주입" .-> web
    envfile -. "환경 변수 주입" .-> sch
    pg --- pgvol
    ora --- oravol
```

### 2.2 구성 요소별 역할

| 구성 요소 | 역할 | 비고 |
|---|---|---|
| `airflow-webserver` | DAG 모니터링, 수동 실행, Connection 관리 UI | Oracle에 직접 접속하지 않음 |
| `airflow-scheduler` | DAG 파싱, 스케줄 판단, Task 실행 | LocalExecutor이므로 Task가 Scheduler 컨테이너 안에서 서브프로세스로 실행됨. **Oracle 접속 주체** |
| `airflow-postgres` | DAG Run·Task Instance 상태, 사용자 계정, Variable 저장 | 호스트 포트 미노출 |
| `oracle-db` | 원천 테이블과 마트 테이블 저장 | PDB `FREEPDB1` 사용 |
| `info-net` | 컨테이너 간 통신용 bridge 네트워크 | 서비스명(`oracle-db`, `postgres`)으로 DNS 해석 |
| `.env` | Compose 변수 파일. Oracle Connection 정의 | 비밀번호 포함. 버전 관리 대상에서 제외해야 함 |

---

## 3. 기술 스택 및 인프라 명세

### 3.1 기술 스택

| 구분 | 구성 요소 | 버전 / 이미지 | 비고 |
|---|---|---|---|
| Host OS | Windows 11 Pro + WSL2 (Ubuntu) | 10.0.26200 | Docker Desktop의 WSL2 백엔드 사용 |
| 컨테이너 플랫폼 | Docker Desktop / Docker Compose v2 | — | Compose 파일의 `version: '3.8'`은 무시됨(경고만 출력) |
| 오케스트레이터 | Apache Airflow | `apache/airflow:2.8.1-python3.10` | Python 3.10 |
| Executor | LocalExecutor | — | 단일 노드 병렬 실행 |
| 메타데이터 DB | PostgreSQL | `postgres:13` | DB/계정: `airflow` |
| 대상 DB | Oracle Database Free | `container-registry.oracle.com/database/free:latest` | 실제 기동 버전: **Oracle AI Database 26ai Free 23.26.3.0.0** (23c → 23ai → 26ai로 이어지는 23 계열) |
| Oracle Provider | `apache-airflow-providers-oracle` | Airflow 2.8.1 constraints 기준 | 컨테이너 기동 시 `_PIP_ADDITIONAL_REQUIREMENTS`로 설치 |
| SQL Operator | `SQLExecuteQueryOperator` | `apache-airflow-providers-common-sql` | |

### 3.2 컨테이너 및 포트

| 컨테이너 | 이미지 | 호스트 포트 | 컨테이너 내부 주소 | 재시작 정책 | 의존성 |
|---|---|---|---|---|---|
| `oracle-db` | `database/free:latest` | `1521` → `1521` | `oracle-db:1521` | 없음 | — |
| `airflow-postgres` | `postgres:13` | 미노출 | `postgres:5432` | 없음 | — |
| `airflow-webserver` | `apache/airflow:2.8.1-python3.10` | `8080` → `8080` | `airflow-webserver:8080` | `always` | `postgres`, `oracle-db` |
| `airflow-scheduler` | `apache/airflow:2.8.1-python3.10` | 미노출 | — | `always` | `airflow-webserver` |

> `depends_on`은 **기동 순서만** 보장하며 Oracle이 접속 가능한 상태(healthy)가 될 때까지 기다리지 않습니다. Oracle 기동에는 시간이 걸리므로(빈 볼륨에서 최초 생성 시 수 분), 기동 직후 실행된 Task는 접속 오류로 실패할 수 있습니다(재시도 1회 설정으로 일부 완화).

### 3.3 스토리지 구성

| 대상 | 유형 | 호스트 측 / 볼륨명 | 컨테이너 경로 | 영속성 |
|---|---|---|---|---|
| DAG 코드 | Bind mount | `./dags` | `/opt/airflow/dags` | 호스트 파일이므로 유지 |
| Task 로그 | Bind mount | `./logs` | `/opt/airflow/logs` | 호스트 파일이므로 유지 |
| 플러그인 | Bind mount | `./plugins` | `/opt/airflow/plugins` | 호스트 파일이므로 유지 |
| Airflow 메타데이터 | **Named Volume** | `welcome-project_postgres_data` | `/var/lib/postgresql/data` | `down`/`up`, 컨테이너 재생성 후에도 유지 |
| Oracle 데이터파일 | **Named Volume** | `welcome-project_oracle_data` | `/opt/oracle/oradata` | `down`/`up`, 컨테이너 재생성 후에도 유지 |

볼륨명의 `welcome-project_` 접두어는 Compose 프로젝트명(프로젝트 디렉터리명)입니다. 디렉터리 이름을 바꾸거나 `-p` 옵션으로 프로젝트명을 바꾸면 **다른 빈 볼륨**이 연결되니 주의하세요.

### 3.4 주요 Airflow 환경 변수

| 변수 | 값 | 정의 위치 | 의미 |
|---|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | `docker-compose.yml` | Executor 종류 |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres/airflow` | `docker-compose.yml` (비밀번호는 `.env`) | 메타데이터 DB 접속 정보 |
| `AIRFLOW__CORE__FERNET_KEY` | `${FERNET_KEY}` | `.env` → `docker-compose.yml` | 메타데이터 DB에 저장된 비밀번호 암호화 키. **변경하면 저장된 비밀번호를 복호화할 수 없음** |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `False` | `docker-compose.yml` | 예제 DAG 미로딩 |
| `_PIP_ADDITIONAL_REQUIREMENTS` | `apache-airflow-providers-oracle --constraint ...` | `docker-compose.yml` | 기동 시 Oracle Provider 설치 |
| `AIRFLOW_CONN_ORACLE_DEFAULT` | JSON 형식 Connection | `.env` → `docker-compose.yml`에서 `${...}`로 주입 | `oracle_default` Connection 정의 ([6.2](#62-connection-관리-oracle_default) 참고) |
| `AIRFLOW__CORE__DEFAULT_TIMEZONE` | 미설정 (기본값 `utc`) | — | 타임존을 지정하지 않은 DAG의 기본값. 본 DAG는 `start_date`에 `Asia/Seoul`을 지정해 이 설정의 영향을 받지 않음 |

### 3.5 프로젝트 디렉터리 구조

```
welcome-project/
├── .env                         # 비밀번호·Fernet Key·Connection 정의 (버전 관리 제외)
├── .env.example                 # .env 템플릿 (값 없음, 버전 관리 대상)
├── .gitignore
├── docker-compose.yml           # 컨테이너·네트워크·볼륨 정의 (비밀 값은 .env 참조)
├── dags/
│   └── loan_summary_dag.py      # ETL DAG
├── sql/
│   └── ddl/
│       └── tb_loan_transaction.sql   # 원천 테이블 DDL
├── backup/
│   ├── export_source.sql        # 원천 데이터 → INSERT 문 추출 스크립트
│   └── restore_source.sql       # 추출 결과 (2026-10-05 기준 10건, 버전 관리 제외)
├── docs/
│   ├── ARCHITECTURE.md          # 본 문서
│   └── worklog/                 # 일자별 작업 로그
├── logs/                        # Airflow Task 로그 (Bind mount, 버전 관리 제외)
└── plugins/                     # Airflow 플러그인 (Bind mount, 현재 비어 있음)
```

---

## 4. 데이터 파이프라인 흐름

### 4.1 DAG 속성

| 항목 | 값 |
|---|---|
| DAG ID | `loan_transaction_summary_dag` |
| 소스 파일 | [`dags/loan_summary_dag.py`](../dags/loan_summary_dag.py) |
| Owner | `welcome_bank` (UI 표시용 라벨이며 실행·권한에는 영향 없음) |
| 타임존 | `Asia/Seoul` (`start_date`의 타임존이 DAG 타임존이 됨) |
| 스케줄 | `schedule='0 1 * * *'` — 매일 **01:00 KST** |
| 시작일 | `pendulum.datetime(2026, 10, 1, tz=local_tz)` |
| `catchup` | `False` |
| `depends_on_past` | `False` |
| 재시도 | 1회, 간격 5분 (Sensor timeout으로 인한 실패는 재시도하지 않음) |
| Connection ID | `oracle_default` |
| 태그 | `loan`, `summary`, `phase3`, `dm` |

### 4.2 배치 기준일자

각 DAG Run은 **데이터 구간 시작 시각을 KST로 바꾼 날짜(`YYYYMMDD`) 하루치**를 처리합니다. Sensor와 MERGE가 같은 식을 사용합니다.

```sql
WHERE TX_DATE = '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}'
```

| 실행 유형 | 실행 시각 (KST) | 데이터 구간 (KST) | 배치 기준일자 |
|---|---|---|---|
| 정기 실행 | 10/07 01:00 | 10/06 01:00 ~ 10/07 01:00 | `20261006` (전일) |
| 수동 실행 (Trigger) | 10/05 17:40 | 10/04 01:00 ~ 10/05 01:00 | `20261004` (직전에 끝난 구간) |
| Backfill | 지정 | 지정한 구간 | 구간 시작일 |

**`{{ ds }}`를 쓰지 않는 이유**: `ds`는 DAG 타임존과 관계없이 **UTC 기준** 날짜입니다. KST 01:00에 시작하는 구간은 UTC로 전날 16:00이므로, `ds`를 쓰면 기준일자가 하루 더 이전으로 밀립니다(예: 10/07 01:00 실행 → `ds = 2026-10-05`).

**특정 일자 재처리**: Grid 화면에서 해당 Run을 Clear하거나, 구간 시작 시각을 KST 오프셋과 함께 지정해 Backfill합니다.

```bash
# 20261003 재처리 (--dry-run으로 렌더링 결과를 먼저 확인 가능)
airflow dags backfill loan_transaction_summary_dag \
  -s 2026-10-03T01:00:00+09:00 -e 2026-10-03T01:00:00+09:00
```

### 4.3 Task 의존성

```mermaid
flowchart LR
    t1["create_dm_table<br/>SQLExecuteQueryOperator<br/>(마트 DDL)"]
    t2["wait_for_source_data<br/>SqlSensor<br/>(원천 도착 대기)"]
    t3["aggregate_and_load_dm<br/>SQLExecuteQueryOperator<br/>(기준일자 집계 + MERGE)"]
    t1 --> t3
    t2 --> t3
```

`create_dm_table`과 `wait_for_source_data`는 서로 의존하지 않으므로 병렬로 실행됩니다. 둘 다 성공해야 `aggregate_and_load_dm`이 실행됩니다.

| Task ID | Operator | 동작 | 실패·재실행 시 동작 |
|---|---|---|---|
| `create_dm_table` | `SQLExecuteQueryOperator` | PL/SQL 익명 블록에서 `EXECUTE IMMEDIATE`로 `CREATE TABLE` 실행 | 테이블이 있으면 `ORA-00955`를 무시하고 성공 처리. 그 외 오류는 `RAISE` |
| `wait_for_source_data` | `SqlSensor` | 배치 기준일자의 원천 건수(`COUNT(1)`)가 1 이상인지 확인 | 3시간 안에 데이터가 없으면 `AirflowSensorTimeout`으로 실패하고 재시도하지 않음. 후속 Task는 `upstream_failed` |
| `aggregate_and_load_dm` | `SQLExecuteQueryOperator` | 배치 기준일자 원천을 집계한 뒤 `MERGE INTO`로 마트 Upsert | 재실행해도 같은 결과(멱등). 실패 시 5분 후 1회 재시도 |

**Sensor 설정**

| 속성 | 값 | 근거 |
|---|---|---|
| `mode` | `reschedule` | 대기하는 동안 Worker 슬롯을 반납하므로 다른 Task 실행을 막지 않음 |
| `poke_interval` | 300초 (5분) | reschedule 모드는 확인할 때마다 Scheduler가 Task를 다시 잡으므로 1분을 넘는 간격을 권장함 |
| `timeout` | 10,800초 (3시간) | 01:00 → 04:00 KST까지 대기. 원천 적재 SLA가 정해지면 그에 맞춰 조정 |

### 4.4 데이터 흐름

```mermaid
flowchart LR
    src[("SYSTEM.TB_LOAN_TRANSACTION<br/>원천 · 거래 단위")]
    sensor{{"SqlSensor<br/>기준일자 COUNT ≥ 1 ?"}}
    agg["집계 (USING 서브쿼리)<br/>WHERE TX_DATE = 기준일자<br/>GROUP BY TX_DATE, PRODUCT_CODE<br/>CASE WHEN TX_TYPE별 COUNT / SUM"]
    merge{"MERGE INTO<br/>ON SUMMARY_DATE + PRODUCT_CODE"}
    upd["WHEN MATCHED<br/>UPDATE 집계값 + LOAD_TIME"]
    ins["WHEN NOT MATCHED<br/>INSERT 신규 행"]
    dm[("SYSTEM.DM_LOAN_TRANSACTION_SUMMARY<br/>마트 · 일자 × 상품")]

    src -. "도착 확인" .-> sensor
    sensor -->|"성공"| agg
    src --> agg --> merge
    merge --> upd --> dm
    merge --> ins --> dm
```

렌더링된 MERGE를 `EXPLAIN PLAN`으로 확인한 결과, 기준일자 조건이 리터럴 비교 `filter("TX_DATE"='YYYYMMDD')`로 적용됩니다. 다만 `TX_DATE` 인덱스가 없어 원천은 전체 스캔(`TABLE ACCESS FULL`)합니다.

### 4.5 집계 로직 (컬럼 매핑)

| 마트 컬럼 | 산출식 | 원천 조건 |
|---|---|---|
| `SUMMARY_DATE` | `TX_DATE` | GROUP BY 키 |
| `PRODUCT_CODE` | `PRODUCT_CODE` | GROUP BY 키 |
| `NEW_CNT` | `COUNT(CASE WHEN TX_TYPE = 'NEW' THEN 1 END)` | 신규 실행 건수 |
| `NEW_AMT` | `NVL(SUM(CASE WHEN TX_TYPE = 'NEW' THEN TX_AMOUNT ELSE 0 END), 0)` | 신규 실행 금액 |
| `REPAY_CNT` | `COUNT(CASE WHEN TX_TYPE = 'REPAY' THEN 1 END)` | 상환 건수 |
| `REPAY_AMT` | `NVL(SUM(CASE WHEN TX_TYPE = 'REPAY' THEN TX_AMOUNT ELSE 0 END), 0)` | 상환 금액 |
| `INTEREST_AMT` | `NVL(SUM(CASE WHEN TX_TYPE = 'INTEREST' THEN TX_AMOUNT ELSE 0 END), 0)` | 이자 금액 (건수는 미집계) |
| `LOAD_TIME` | `SYSTIMESTAMP` | UPDATE 시 명시 갱신, INSERT 시 컬럼 기본값 적용 |

### 4.6 현재 적재 방식의 특성과 한계

| 항목 | 내용 |
|---|---|
| 적재 범위 | **증분 적재**. 실행마다 배치 기준일자 하루치만 집계함 |
| 멱등성 | 보장됨. 같은 Run을 다시 실행하면 같은 기준일자를 같은 결과로 MERGE함 |
| 원천 도착 판정 | 기준일자 건수가 1건 이상이면 통과하므로, 원천이 **일부만 적재된 상태**에서도 집계가 실행될 수 있음 |
| 지연·정정 데이터 | 이미 처리한 일자의 원천이 나중에 추가·수정되면 자동으로 반영되지 않음. 해당 일자 Run을 Clear하거나 Backfill해야 함 |
| 원천 삭제 반영 | 반영되지 않음. 원천에서 특정 일자·상품의 거래가 모두 삭제되어도 마트의 기존 행은 남음 |
| 미정의 거래유형 | `NEW`/`REPAY`/`INTEREST` 외의 `TX_TYPE`은 어느 컬럼에도 집계되지 않고 조용히 누락됨 |
| 원천 조회 성능 | `TX_DATE` 인덱스가 없어 전체 스캔. 원천이 커지면 인덱스 필요 ([6.4](#64-향후-고도화-방향) 참고) |

---

## 5. 데이터 스키마 정의

### 5.1 원천 테이블: `SYSTEM.TB_LOAN_TRANSACTION`

- **Grain**: 거래 1건 = 1행
- **DDL 관리**: [`sql/ddl/tb_loan_transaction.sql`](../sql/ddl/tb_loan_transaction.sql)
- **데이터 현황**: 10건, `TX_DATE` 20261001 ~ 20261004 (2026-10-05 기준)

| # | 컬럼 | 타입 | NULL | 키 / 기본값 | 설명 |
|---|---|---|---|---|---|
| 1 | `TX_SEQ` | `NUMBER` | N | **PK**, `GENERATED ALWAYS AS IDENTITY` | 거래 일련번호 (자동 채번, 직접 입력 불가) |
| 2 | `TX_DATE` | `VARCHAR2(8)` | N | | 거래일자 (`YYYYMMDD`) |
| 3 | `CUST_ID` | `VARCHAR2(10)` | N | | 고객 ID |
| 4 | `PRODUCT_CODE` | `VARCHAR2(10)` | N | | 상품 코드 |
| 5 | `TX_TYPE` | `VARCHAR2(10)` | N | | 거래 유형: `NEW`(신규), `REPAY`(상환), `INTEREST`(이자). CHECK 제약 없음 |
| 6 | `TX_AMOUNT` | `NUMBER(15,2)` | N | | 거래 금액 |
| 7 | `CREATE_TIME` | `TIMESTAMP(6)` | Y | `DEFAULT SYSTIMESTAMP` | 행 생성 시각 |

**제약·인덱스**
- PK 제약은 이름 없이 선언되어 Oracle이 시스템 이름(`SYS_C00xxxx`)을 붙입니다. 테이블을 다시 만들 때마다 이름이 바뀝니다(2026-10-05 재생성 후 `SYS_C008658`).
- 인덱스는 PK 인덱스(`TX_SEQ`)만 있습니다. `TX_DATE` 인덱스가 없어 날짜 조건으로 조회하면 전체 스캔이 발생합니다.

### 5.2 타겟 마트 테이블: `SYSTEM.DM_LOAN_TRANSACTION_SUMMARY`

- **Grain**: 거래일자 × 상품코드 = 1행
- **DDL 관리**: DAG Task `create_dm_table` 안에 정의됨
- **데이터 현황**: 10건 (2026-10-05 기준)

| # | 컬럼 | 타입 | NULL | 키 / 기본값 | 설명 |
|---|---|---|---|---|---|
| 1 | `SUMMARY_DATE` | `VARCHAR2(8)` | N | **PK** (`PK_DM_LOAN_DAILY`, 복합) | 기준일자 (`YYYYMMDD`, 원천 `TX_DATE`) |
| 2 | `PRODUCT_CODE` | `VARCHAR2(10)` | N | **PK** (`PK_DM_LOAN_DAILY`, 복합) | 상품 코드 |
| 3 | `NEW_CNT` | `NUMBER(10)` | Y | `DEFAULT 0` | 신규 실행 건수 |
| 4 | `NEW_AMT` | `NUMBER(15,2)` | Y | `DEFAULT 0` | 신규 실행 금액 합계 |
| 5 | `REPAY_CNT` | `NUMBER(10)` | Y | `DEFAULT 0` | 상환 건수 |
| 6 | `REPAY_AMT` | `NUMBER(15,2)` | Y | `DEFAULT 0` | 상환 금액 합계 |
| 7 | `INTEREST_AMT` | `NUMBER(15,2)` | Y | `DEFAULT 0` | 이자 금액 합계 |
| 8 | `LOAD_TIME` | `TIMESTAMP(6)` | Y | `DEFAULT SYSTIMESTAMP` | 마지막 적재(갱신) 시각 |

### 5.3 원천 → 마트 관계

```mermaid
flowchart LR
    subgraph SRC["TB_LOAN_TRANSACTION (N)"]
        s1["TX_DATE"]
        s2["PRODUCT_CODE"]
        s3["TX_TYPE"]
        s4["TX_AMOUNT"]
    end
    subgraph DM["DM_LOAN_TRANSACTION_SUMMARY (1)"]
        d1["SUMMARY_DATE (PK)"]
        d2["PRODUCT_CODE (PK)"]
        d3["NEW_CNT / NEW_AMT"]
        d4["REPAY_CNT / REPAY_AMT"]
        d5["INTEREST_AMT"]
    end
    s1 --> d1
    s2 --> d2
    s3 -->|"유형 분기"| d3 & d4 & d5
    s4 -->|"합계"| d3 & d4 & d5
```

---

## 6. 운영 고려사항

### 6.1 볼륨 영속성

#### 현재 구성

```yaml
services:
  oracle-db:
    volumes:
      - oracle_data:/opt/oracle/oradata
  postgres:
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  oracle_data:
  postgres_data:
```

2026-10-05에 익명 볼륨(Postgres)과 컨테이너 쓰기 레이어(Oracle)에서 위 Named Volume으로 전환했습니다. 전환 절차와 검증 결과는 [작업 로그](worklog/2026-10-05.md)에 있습니다.

#### 명령어별 영향

| 명령어 | Oracle 데이터 | Airflow 메타데이터 | 사용 여부 |
|---|---|---|---|
| `docker compose stop` / `start` | 유지 | 유지 | ✅ 일상적인 중지·재개 |
| `docker compose restart` | 유지 | 유지 | ✅ |
| `docker compose up -d` (설정 변경으로 컨테이너 재생성) | 유지 | 유지 | ✅ |
| `docker compose down` → `up -d` | 유지 | 유지 | ✅ 컨테이너 재구성 시 사용 |
| `docker compose down -v` / `--volumes` | **삭제** | **삭제** | ❌ **사용 금지** |
| `docker volume prune -a` | **삭제** (컨테이너가 내려가 있을 때) | **삭제** | ❌ 사용 금지 |

> 볼륨 삭제는 되돌릴 수 없습니다. 볼륨을 정리해야 할 때는 `docker volume ls`로 대상을 확인한 뒤 개별로 `docker volume rm <이름>`을 실행하세요. `docker volume rm`은 실행 중인 컨테이너가 사용하는 볼륨은 삭제하지 않습니다.

#### 백업

**① 논리 백업 (원천 데이터만, 권장)**: 볼륨 상태와 무관하게 SQL 파일로 보관합니다. PowerShell 기준입니다.

```powershell
docker cp backup/export_source.sql oracle-db:/tmp/
docker exec oracle-db sqlplus -s "system/<ORACLE_PWD>@FREEPDB1" "@/tmp/export_source.sql"
docker cp oracle-db:/tmp/restore_source.sql backup/
```

복구는 `sql/ddl/tb_loan_transaction.sql` → `backup/restore_source.sql` 순서로 실행합니다. `TX_SEQ`는 `GENERATED ALWAYS`이므로 복구 시 1부터 다시 채번됩니다. 마트는 DAG를 다시 실행하면 재생성됩니다.

**② 물리 백업 (볼륨 전체)**: Oracle을 중지한 상태에서 볼륨을 tar로 묶습니다.

```powershell
docker compose stop oracle-db
docker run --rm -v welcome-project_oracle_data:/data -v "${PWD}/backup:/backup" alpine tar czf /backup/oracle_data.tar.gz -C /data .
docker compose start oracle-db
```

### 6.2 Connection 관리 (`oracle_default`)

#### 현재 정의 방식

`oracle_default`는 `.env`의 `AIRFLOW_CONN_ORACLE_DEFAULT`로 정의되고, `docker-compose.yml`에서 Webserver·Scheduler 두 컨테이너에 주입됩니다.

```bash
# .env (비밀번호 포함 — 버전 관리 제외)
AIRFLOW_CONN_ORACLE_DEFAULT='{"conn_type": "oracle", "host": "oracle-db", "port": 1521, "login": "SYSTEM", "password": "<ORACLE_PWD>", "extra": {"service_name": "FREEPDB1"}}'
```

```yaml
# docker-compose.yml — airflow-webserver, airflow-scheduler 공통
environment:
  - AIRFLOW_CONN_ORACLE_DEFAULT=${AIRFLOW_CONN_ORACLE_DEFAULT}
```

| 필드 | 값 | 비고 |
|---|---|---|
| Connection Type | `oracle` | |
| Host | `oracle-db` | 컨테이너 안에서 `localhost`를 쓰면 자기 자신을 가리키므로 접속 불가 |
| Port | `1521` | 컨테이너 내부 포트 |
| Login | `SYSTEM` | 전용 계정 전환 권장 (아래 '보안 및 계정 권한') |
| Schema | (비움) | 서비스명은 Schema가 아니라 Extra로 전달 |
| Extra | `{"service_name": "FREEPDB1"}` | PDB 서비스명 |

**동작 규칙**
- 환경 변수 Connection이 메타데이터 DB에 저장된 같은 ID의 Connection보다 **우선**합니다. Scheduler에서 조회하면 `.env` 값(Schema 없음, `service_name` 사용)으로 해석되는 것을 확인했습니다.
- 메타데이터 DB에는 `oracle_default`가 없습니다. 전환 전에 UI로 등록했던 항목은 2026-10-05에 삭제했습니다. 따라서 **UI의 Connection 목록에는 `oracle_default`가 표시되지 않는 것이 정상**이며, 값을 바꾸려면 `.env`를 수정해야 합니다.
- `.env`를 수정한 뒤에는 `docker compose up -d`로 컨테이너를 재생성해야 반영됩니다(`restart`로는 환경 변수가 바뀌지 않음).
- 호스트(Windows)의 SQL 클라이언트에서는 `localhost:1521/FREEPDB1`로 접속합니다.

#### 보안 및 계정 권한

| 항목 | 현재 | 권장 |
|---|---|---|
| DB 계정 | `SYSTEM` (DBA 권한) | 원천 `SELECT`, 마트 `SELECT/INSERT/UPDATE`만 가진 전용 계정 |
| 테이블 소유 스키마 | `SYSTEM` | 업무 전용 스키마 (예: `LOAN_DW`) |
| 비밀 정보 (`ORACLE_PWD`, `POSTGRES_PASSWORD`, `FERNET_KEY`, Oracle Connection) | ✅ `.env`에만 저장, `.gitignore`로 버전 관리 제외. compose는 `${VAR:?}`로 참조해 값이 없으면 기동 거부 | 운영 환경에서는 Secrets Backend(Vault 등) 검토 |
| 백업 파일 (`backup/restore_source.sql`) | ✅ `.gitignore`로 버전 관리 제외 (추출 스크립트만 관리) | |
| Claude Code 로컬 설정 (`.claude/settings*.json`) | ✅ 버전 관리 제외 (명령 이력에 비밀번호·로컬 경로 포함) | |

새 환경에서는 `.env.example`을 `.env`로 복사한 뒤 값을 채우고 기동합니다.

### 6.3 실행 환경 관련 주의사항

| 항목 | 현재 상태 | 영향 | 권장 조치 |
|---|---|---|---|
| 템플릿 날짜 변수 | `ds`, `ds_nodash` 등은 UTC 기준 | KST DAG에서 그대로 쓰면 기준일자가 하루 밀림 | KST 날짜가 필요하면 `data_interval_start.in_timezone(dag.timezone)` 사용 ([4.2](#42-배치-기준일자)) |
| Provider 설치 방식 | `_PIP_ADDITIONAL_REQUIREMENTS` | 컨테이너가 기동할 때마다 pip 설치 → 기동 지연, 외부 네트워크 의존 | `apache/airflow:2.8.1-python3.10` 기반 커스텀 이미지(Dockerfile)로 Provider 사전 설치 |
| Oracle 이미지 태그 | `latest` | 이미지를 다시 pull 하면 버전이 바뀔 수 있음 (현재 23.26.3). 볼륨의 데이터파일과 버전이 어긋날 위험 | 버전 태그 고정 |
| Airflow 초기화 | `airflow-init` 서비스 없음 | 메타데이터 볼륨을 새로 만들 때 DB 초기화와 관리자 계정 생성을 수동으로 수행해야 함 | 공식 Compose의 `airflow-init` 패턴 도입 |
| Oracle 기동 대기 | `depends_on` 기동 순서만 보장 | Oracle 준비 전 Task 실패 가능 | `depends_on.condition: service_healthy` + Oracle healthcheck 설정 |
| Compose `version` 속성 | `version: '3.8'` 남아 있음 | 실행할 때마다 "obsolete" 경고 출력 (동작 영향 없음) | 1번째 줄 삭제 |

### 6.4 향후 고도화 방향

#### 적용 현황

| 항목 | 상태 | 비고 |
|---|---|---|
| `MERGE INTO` 기반 Upsert | ✅ 적용 | |
| 증분 적재 (배치 기준일자) | ✅ 적용 (2026-10-05) | [4.2](#42-배치-기준일자) 참고 |
| 원천 도착 감지 (`SqlSensor`) | ✅ 적용 (2026-10-05) | [4.3](#43-task-의존성) 참고 |
| `TX_DATE` 인덱스 | 미적용 | 아래 1) |
| 지연·정정 데이터 재집계 | 미적용 | 아래 1) |
| 원천 적재 완료 플래그 기반 감지 | 미적용 | 아래 2) |
| 데이터 품질 검증 | 미적용 | 아래 3) |

#### 1) 증분 적재 보완

```sql
-- 날짜 조건 조회 성능을 위한 인덱스
CREATE INDEX SYSTEM.IX_TB_LOAN_TX_DATE ON SYSTEM.TB_LOAN_TRANSACTION (TX_DATE);
```

- **지연 도착·정정 데이터**: 원천이 며칠 늦게 들어오거나 수정되는 경우가 있다면, 기준일자 하루가 아니라 최근 N일을 다시 집계하는 방식을 검토합니다.
  ```sql
  WHERE TX_DATE BETWEEN '{{ (data_interval_start.in_timezone(dag.timezone) - macros.timedelta(days=2)).strftime("%Y%m%d") }}'
                    AND '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}'
  ```
  이 경우 Sensor는 기준일자만 확인하도록 그대로 둡니다.
- **원천 삭제 반영**: 원천 삭제를 마트에 반영해야 한다면, 대상 일자의 마트 행을 먼저 `DELETE`한 뒤 `INSERT`하는 방식(파티션 교체형)을 검토합니다.
- **기준일자 식 중복 제거**: 같은 Jinja 식이 Sensor와 MERGE에 반복됩니다. 사용처가 늘어나면 `user_defined_macros`로 함수화하는 것을 검토합니다.

#### 2) 원천 적재 완료 감지 고도화

현재 Sensor는 기준일자 건수가 1건 이상이면 통과하므로, 원천이 **일부만 적재된 상태**에서도 집계가 실행될 수 있습니다. 원천 시스템이 적재 완료 플래그(제어 테이블)를 제공한다면 Sensor SQL을 그 플래그를 확인하는 방식으로 바꾸는 것이 더 안전합니다.

```sql
-- 예시: 원천 시스템의 적재 완료 제어 테이블을 확인
SELECT COUNT(1) FROM SYSTEM.TB_BATCH_CONTROL
WHERE JOB_NAME = 'LOAN_TRANSACTION'
  AND BASE_DATE = '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}'
  AND STATUS = 'DONE'
```

#### 3) 데이터 품질 검증 (Data Quality Check)

적재 후 원천과 마트의 금액 합계를 대사합니다. `NEW`/`REPAY`/`INTEREST` 외의 거래유형이 들어오면 합계가 맞지 않아 실패하므로, 미정의 유형도 함께 감지할 수 있습니다.

```python
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

dq_reconcile_amount = SQLCheckOperator(
    task_id='dq_reconcile_amount',
    conn_id='oracle_default',
    sql="""
    SELECT CASE WHEN s.amt = d.amt THEN 1 ELSE 0 END
    FROM (SELECT NVL(SUM(TX_AMOUNT), 0) amt
            FROM SYSTEM.TB_LOAN_TRANSACTION
           WHERE TX_DATE = '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}') s,
         (SELECT NVL(SUM(NEW_AMT + REPAY_AMT + INTEREST_AMT), 0) amt
            FROM SYSTEM.DM_LOAN_TRANSACTION_SUMMARY
           WHERE SUMMARY_DATE = '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}') d
    """,
)
```

추가로 검토할 검증 항목:

| 검증 | 방법 |
|---|---|
| 금액 음수 여부 | `SQLColumnCheckOperator`로 `NEW_AMT`, `REPAY_AMT`, `INTEREST_AMT`의 `min >= 0` 확인 |
| 마트 행 존재 여부 | `SQLTableCheckOperator`로 기준일자 `COUNT(*) > 0` 확인 |
| 거래유형 도메인 | 원천 `TX_TYPE`에 `CHECK (TX_TYPE IN ('NEW','REPAY','INTEREST'))` 제약 추가 |

#### 4) 목표 DAG 구조

현재 구조에 품질 검증 Task를 마지막 단계로 추가하는 형태입니다.

```mermaid
flowchart LR
    c["create_dm_table<br/>DDL"]
    s["wait_for_source_data<br/>SqlSensor"]
    a["aggregate_and_load_dm<br/>증분 MERGE (KST 기준일자)"]
    q["dq_reconcile_amount<br/>SQLCheckOperator<br/>(미적용)"]
    c --> a
    s --> a
    a --> q
```

#### 5) 기타 개선 항목

| 항목 | 내용 |
|---|---|
| 장애 알림 | `on_failure_callback` 또는 이메일·Slack 알림 연동 |
| DDL 버전 관리 | 원천 DDL은 `sql/ddl/`로 이관 완료. 마트 DDL도 DAG 문자열에서 `.sql` 파일로 분리해 DAG에서 참조 |
| 이자 건수 | 필요 시 마트에 `INTEREST_CNT` 컬럼 추가 |
| 테스트 | DAG import 오류 검사(`DagBag`)와 집계 SQL 단위 테스트를 CI에 추가 |
| 의존성 명세 | Provider 목록이 compose의 `_PIP_ADDITIONAL_REQUIREMENTS`에 있음. 커스텀 이미지(6.3)로 전환할 때 `requirements.txt`로 분리 |
