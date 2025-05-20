FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scenario5.py .
RUN python -m nltk.downloader stopwords -d /usr/local/nltk_data
EXPOSE 5000

CMD ["python", "scenario5.py"]
