FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
# Dependencies first so code changes do not invalidate the pip layer.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
# .dockerignore keeps .env, .venv, and generated data out of the image.
COPY . /app
CMD ["python", "-m", "src.cli", "validate-env", "--require-db"]
