FROM python:3.14-rc-slim

WORKDIR /app
COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .

EXPOSE 8000
CMD ["uvicorn", "reliqueue.api:app", "--host", "0.0.0.0", "--port", "8000"]
