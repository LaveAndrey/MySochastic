# Используем официальный образ Python
FROM python:3.10

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файлы в контейнер
COPY . .

# Устанавливаем зависимости, если они есть
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем команду по умолчанию
CMD ["python", "mainbinance.py"]
