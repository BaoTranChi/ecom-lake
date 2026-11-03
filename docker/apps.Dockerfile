# Image Python cho các dịch vụ ngoài Spark: producer Kafka, rule engine cảnh báo, API, dashboard, vẽ biểu đồ.
FROM python:3.11-slim
RUN pip install --no-cache-dir \
      kafka-python==2.0.2 requests==2.32.3 \
      fastapi==0.115.6 uvicorn==0.32.1 \
      pandas==2.2.3 tabulate==0.9.0 streamlit==1.39.0 plotly==5.24.1 matplotlib==3.9.2 networkx==3.4.2 \
      pytest==8.3.4
WORKDIR /app
ENV PYTHONUNBUFFERED=1
