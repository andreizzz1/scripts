FROM python:3.11-slim

WORKDIR /app

COPY pybot/pyproject.toml /app/pybot/pyproject.toml
COPY pybot/README.md /app/pybot/README.md
COPY pybot/dickgrowerbot /app/pybot/dickgrowerbot
COPY migrations /app/migrations
COPY locales /app/locales
COPY src/help /app/src/help
COPY src/handlers/privacy /app/src/handlers/privacy

RUN pip install --no-cache-dir -U pip && pip install --no-cache-dir -e /app/pybot

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "dickgrowerbot.main"]

