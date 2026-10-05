from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

# DAG 타임존(KST). 스케줄 해석과 SQL의 배치 기준일자 계산(dag.timezone)에 함께 사용
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'welcome_bank',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'loan_transaction_summary_dag',
    default_args=default_args,
    description='여신 거래 원천 데이터를 일별/상품별로 집계하여 DM 테이블에 적재하는 ETL 파이프라인',
    schedule='0 1 * * *',  # 매일 01:00 KST 실행 (start_date의 타임존 기준)
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    catchup=False,
    tags=['loan', 'summary', 'phase3', 'dm'],  
) as dag:

    # 1. Target 데이터마트(DM) 테이블이 없으면 생성
    create_dm_table = SQLExecuteQueryOperator(
        task_id='create_dm_table',
        conn_id='oracle_default',
        sql="""
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE SYSTEM.DM_LOAN_TRANSACTION_SUMMARY (
                    SUMMARY_DATE    VARCHAR2(8) NOT NULL,
                    PRODUCT_CODE    VARCHAR2(10) NOT NULL,
                    NEW_CNT         NUMBER(10) DEFAULT 0,
                    NEW_AMT         NUMBER(15, 2) DEFAULT 0,
                    REPAY_CNT       NUMBER(10) DEFAULT 0,
                    REPAY_AMT       NUMBER(15, 2) DEFAULT 0,
                    INTEREST_AMT    NUMBER(15, 2) DEFAULT 0,
                    LOAD_TIME       TIMESTAMP DEFAULT SYSTIMESTAMP,
                    CONSTRAINT PK_DM_LOAN_DAILY PRIMARY KEY (SUMMARY_DATE, PRODUCT_CODE)
                )
            ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN -- ORA-00955: 테이블이 이미 존재할 경우 무시
                    RAISE;
                END IF;
        END;
        """,
    )

    # 2. 배치 기준일자 원천 데이터 존재 여부 센싱
    wait_for_source_data = SqlSensor(
        task_id='wait_for_source_data',
        conn_id='oracle_default',
        # 배치 기준일자(KST) 원천 데이터 건수가 1건 이상이면 Success로 다음 Task 진행
        # ds는 UTC 기준이므로 데이터 구간 시작일을 DAG 타임존(KST)으로 변환해 사용 (예: 10/07 01:00 실행 → 20261006)
        sql="""
        SELECT COUNT(1)
        FROM SYSTEM.TB_LOAN_TRANSACTION
        WHERE TX_DATE = '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}'
        """,
        poke_interval=60 * 5,   # 5분마다 체크 (reschedule 모드는 1분 초과 간격 권장)
        timeout=60 * 60 * 3,    # 최대 3시간 대기 (01:00 → 04:00 KST). Sensor timeout은 retries 없이 즉시 실패
        mode='reschedule',      # 대기 중에는 Worker 슬롯을 반납하여 다른 Task 실행 가능
    )

    # 3. 원천 데이터(TB_LOAN_TRANSACTION) 집계 후 DM 테이블 적재 (MERGE 문으로 덮어쓰기/신규입력)
    aggregate_and_load_dm = SQLExecuteQueryOperator(
        task_id='aggregate_and_load_dm',
        conn_id='oracle_default',
        sql="""
        MERGE INTO SYSTEM.DM_LOAN_TRANSACTION_SUMMARY T
        USING (
            SELECT
                TX_DATE AS SUMMARY_DATE,
                PRODUCT_CODE,
                COUNT(CASE WHEN TX_TYPE = 'NEW' THEN 1 END) AS NEW_CNT,
                NVL(SUM(CASE WHEN TX_TYPE = 'NEW' THEN TX_AMOUNT ELSE 0 END), 0) AS NEW_AMT,
                COUNT(CASE WHEN TX_TYPE = 'REPAY' THEN 1 END) AS REPAY_CNT,
                NVL(SUM(CASE WHEN TX_TYPE = 'REPAY' THEN TX_AMOUNT ELSE 0 END), 0) AS REPAY_AMT,
                NVL(SUM(CASE WHEN TX_TYPE = 'INTEREST' THEN TX_AMOUNT ELSE 0 END), 0) AS INTEREST_AMT
            FROM SYSTEM.TB_LOAN_TRANSACTION
            WHERE TX_DATE = '{{ data_interval_start.in_timezone(dag.timezone).strftime("%Y%m%d") }}' -- 배치 기준일자(KST) 데이터만 집계
            GROUP BY TX_DATE, PRODUCT_CODE
        ) S
        ON (T.SUMMARY_DATE = S.SUMMARY_DATE AND T.PRODUCT_CODE = S.PRODUCT_CODE)
        WHEN MATCHED THEN
            UPDATE SET
                T.NEW_CNT = S.NEW_CNT,
                T.NEW_AMT = S.NEW_AMT,
                T.REPAY_CNT = S.REPAY_CNT,
                T.REPAY_AMT = S.REPAY_AMT,
                T.INTEREST_AMT = S.INTEREST_AMT,
                T.LOAD_TIME = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (SUMMARY_DATE, PRODUCT_CODE, NEW_CNT, NEW_AMT, REPAY_CNT, REPAY_AMT, INTEREST_AMT)
            VALUES (S.SUMMARY_DATE, S.PRODUCT_CODE, S.NEW_CNT, S.NEW_AMT, S.REPAY_CNT, S.REPAY_AMT, S.INTEREST_AMT)
        """,
    )

    # Task 실행 순서 정의 (테이블 생성 + 원천 대기를 병렬 수행 -> 둘 다 성공하면 집계 및 적재)
    [create_dm_table, wait_for_source_data] >> aggregate_and_load_dm