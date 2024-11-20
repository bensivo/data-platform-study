FROM python:3.11-buster

RUN pip install poetry==1.8.2

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN poetry install

COPY . .

EXPOSE 8000
CMD ["poetry", "run", "fastapi", "run", "main.py"]
