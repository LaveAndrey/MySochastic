# Используем официальный образ Python
FROM python:3.11

RUN apt-get update && apt-get install -y sqlite3

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем только requirements.txt, чтобы слои кешировались
COPY requirements.txt .

# Устанавливаем зависимости (кешируется если requirements.txt не изменился)
RUN pip install --no-cache-dir -r requirements.txt

# Теперь копируем весь остальной код
COPY . .

# Устанавливаем команду по умолчанию
CMD ["python", "mainbinance.py"]
