# 이미지 빌드
1. .env 파일작성
2. docker build --network=host -t c-airflow:latest .

# airflow 실행
1. docker compose -f ./docker-compose-primary.yml up -d airflow-init
2. docker compose -f ./docker-compose-primary.yml up -d
3. docker compose -f ./docker-compose-primary.yml up -d flower
4. docker compose -f ./docker-compose-worker.yml up -d
