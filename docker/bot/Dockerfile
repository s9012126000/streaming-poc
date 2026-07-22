FROM python:3.12-slim

RUN pip install --no-cache-dir \
      "discord.py==2.7.1" \
      confluent-kafka==2.15.0 \
      python-dotenv==1.2.2

WORKDIR /app
COPY utils/ ./utils/
COPY bot/ ./bot/

CMD ["python", "-m", "bot.main"]
