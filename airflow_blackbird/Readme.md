# 이미지 빌드
1. .env 파일작성
2. docker build --network=host -t c-airflow:latest .

# airflow 실행
1. docker compose -f ./docker-compose-primary.yml up -d airflow-init
2. docker compose -f ./docker-compose-primary.yml up -d
3. docker compose -f ./docker-compose-primary.yml up -d flower
4. docker compose -f ./docker-compose-worker.yml up -d


# 설치

- 모든 코드 실행 위치는 최상위 폴더
- 설치실행환경
    - 개인 홈서버 : cpu: 4core | ram : 8Gb | hard : 500gb | ubuntu


# 1. 모든 폴더 초기화

# 2. data  설치

- 설치 목록
    - PostgreSQL 14
    - redis

1. 환경변수 셋팅
    1. lime-flow-data 폴더에 .env 파일 생성후 아래 내용 입력

        ```bash
        # postgresql
        POSTGRES_PORT=
        POSTGRES_USER=
        POSTGRES_PASSWORD=
        POSTGRES_DB=

        # redis
        REDIS_PORT=
        ```

2. 도커로 설치 목록 설치

    ```bash
    # 실행위치: 최상위 폴더

    docker compose -f ./lime-flow-data/docker-compose-data.yml up -d
    ```


# 3. airflow 설치

- 설치 목록
    - airflow-scheduler
    - airflow-webserver
    - airflow-triggerer
    - airflow-cli
    - airflow-init
    - airflow-flower

1. 환경변수 셋팅
    1. lime-flow 폴더에 .env 파일 생성후 아래 내용 입력

        ```bash
            LIME_FLOW_IMAGE=
            LIME_FLOW_WORKER_IMAGE=

            AIRFLOW_PJT_DIR=./airflow
            AIRFLOW_WORKER_DIR=./worker

            AIRFLOW_PORT=

            # login
            _AIRFLOW_DB_UPGRADE='true'
            _AIRFLOW_WWW_USER_CREATE='true'
            _AIRFLOW_WWW_USER_USERNAME=
            _AIRFLOW_WWW_USER_PASSWORD=

            # core
            AIRFLOW__CORE__EXECUTOR=CeleryExecutor
            AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://{}:{}@{}:{}/airflow
            AIRFLOW__CORE__FERNET_KEY=''
            AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'
            AIRFLOW__CORE__LOAD_EXAMPLES='false'
            AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Seoul
            AIRFLOW__CORE__ENABLE_XCOM_PICKLING='true'

            # logging
            AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
            AIRFLOW__LOGGING__CELERY_LOGGING_LEVEL=DEBUG

            # webserver
            AIRFLOW__WEBSERVER__SECRET_KEY=''
            AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Seoul

            # scheduler
            AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK='true'

            # celery
            AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://{}:{}@{}:{}/airflow
            AIRFLOW__CELERY__BROKER_URL=redis://:@{}:{}/0

            # database
            AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://{}:{}@{}:{}/airflow

            # api
            AIRFLOW__API__AUTH_BACKENDS='airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'

            # worker
            DUMB_INIT_SETSID='0'

            # uid
        ```

    2. AIRFLOW_UID, AIRFLOW_GID 작성

        ```bash
        echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> ./lime-flow/.env
        ```


2. lime-flow 도커 이미지 빌드하기

    ```bash
    docker rmi lime-flow:latest
    docker build ./lime-flow -t lime-flow:latest
    ```


1. airflow-init 먼저 실행

    ```bash
    docker compose -f ./lime-flow/docker-compose-primary.yml up -d airflow-init
    ```

2. scheduler, webserver, triggerer 실행

    ```bash
    docker compose -f ./lime-flow/docker-compose-primary.yml up -d
    ```

3. flower 실행

    ```bash
    docker compose -f ./lime-flow/docker-compose-primary.yml up -d flower
    ```

4. FERNET_KEY 및 SECRET_KEY 생성

    ```bash
    # airflow-scheduler 컨테이너에 접속
    docker exec -it lime-flow-scheduler /bin/bash

    # python에 접속해 FERNET_KEY 가져오기
    $ python
    >>> from cryptography.fernet import Fernet
    >>> FERNET_KEY = Fernet.generate_key().decode()
    >>> print(FERNET_KEY)

    # >>> ''

    # python에 접속해 SECRET_KEY 가져오기
    $ python
    >>> import os
    >>> print(os.urandom(16))

    # >>> ''
    ```

    - 획득한 FERNET_KEY, SECRET_KEY 를 lime-flow/.env 환경변수 파일안에 `AIRFLOW__CORE__FERNET_KEY`와 `AIRFLOW__WEBSERVER__SECRET_KEY`에 각각 집어 넣는다.
5. 변경사항을 c-flow-primary-node에 적용

```bash
docker compose -f ./lime-flow/docker-compose-primary.yml up --build --force-recreate -d airflow-init
docker compose -f ./lime-flow/docker-compose-primary.yml up --build --force-recreate -d
docker compose -f ./lime-flow/docker-compose-primary.yml up --build --force-recreate -d flower
```

# 4. lime-flow-worker-node 설치

- 설치목록
    - c-flow-worker-1
    - c-flow-worker-2

1. 환경변수 셋팅
    1. c-flow-primary-node 설치시 환경변수 파일과 동일

1. 도커 이미지 빌드
    1. c-flow-primary-node와 같은 이미지 사용

2. 이미지 실행

```bash
docker compose -f ./docker-compose-worker.yml up -d
```
