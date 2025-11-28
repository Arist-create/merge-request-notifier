FROM python:3.11-slim

WORKDIR /app

COPY merge.py .

RUN pip install requests

CMD ["python", "merge.py"]
