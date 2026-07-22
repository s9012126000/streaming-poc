FROM python:3.12-slim

# 依賴釘死版本、直接寫在 Dockerfile（照 repo 既有慣例，不另開 requirements.txt）
RUN pip install --no-cache-dir \
      fastapi==0.139.2 \
      "uvicorn[standard]==0.51.0" \
      confluent-kafka==2.15.0 \
      trino==0.338.0

WORKDIR /app
COPY utils/ ./utils/
COPY gateway/ ./gateway/

EXPOSE 8000
CMD ["uvicorn", "gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
