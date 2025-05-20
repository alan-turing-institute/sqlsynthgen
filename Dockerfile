FROM python:3.13.3-alpine3.21
RUN apk add bash poetry
WORKDIR /app
ADD . /app
RUN mkdir /pypoetry
ENV POETRY_VIRTUALENVS_PATH=/pypoetry/cache/virtualenv
ENV SHELL=/bin/bash
ENV HOME=/
RUN poetry install
RUN poetry run sqlsynthgen --install-completion bash
WORKDIR /data
CMD ["poetry", "--directory=/app", "shell"]
