FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY analyzer ./analyzer
COPY data ./data

ENV DB_PATH=/data/survey.db
ENV ANALYZER_DB_PATH=/data/analyzer.db
EXPOSE 8000 8001

# 默认启动上游业务库；分析库服务由 docker-compose 用同一镜像覆盖 command 启动
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
