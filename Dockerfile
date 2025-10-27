FROM python:3.11-slim

WORKDIR /app

# Копируем все файлы
COPY web_server.py .
COPY sync_photos_fast.py .
COPY templates ./templates
COPY static ./static
COPY .env .

# Устанавливаем зависимости
RUN pip install --no-cache-dir --break-system-packages xxhash tqdm pillow flask

# Запускаем
CMD ["python", "web_server.py"]
