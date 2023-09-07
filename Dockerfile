FROM python:3.10.7

# System deps
RUN apt-get update -y && apt-get install -y build-essential
RUN pip install --upgrade pip

WORKDIR /app
COPY pyproject.toml .
COPY poetry.lock* .

ENV POETRY_VIRTUALENVS_CREATE=false

RUN pip install poetry && poetry install

COPY . .

RUN poetry install

ENTRYPOINT ["fireagg"]