FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core/ core/
COPY client/ client/
COPY server/ server/
COPY wizard/ wizard/

RUN mkdir -p data/sessions
