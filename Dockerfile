FROM python:3.13.3-alpine3.21
RUN apk add bash poetry
WORKDIR /app
ADD . /app
RUN mkdir /pypoetry
ENV POETRY_VIRTUALENVS_PATH=/pypoetry/cache/virtualenv
ENV SHELL=/bin/bash
ENV HOME=/
RUN poetry install
SHELL ["/bin/bash", "-c"]
# The redirect to /dev/null seems to help shellingham detect bash!
RUN poetry run datafaker --install-completion > /dev/null
WORKDIR /data
CMD ["poetry", "--directory=/app", "shell"]
