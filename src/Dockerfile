FROM python:3.13

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements.txt


RUN  pip install --upgrade pip \
    && pip install -r requirements.txt --no-cache-dir

COPY . .

CMD ["gunicorn", "core.app:app", "-c", "run_prod.py"] 