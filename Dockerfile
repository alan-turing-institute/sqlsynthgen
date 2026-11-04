FROM python:3.13.3-alpine3.22
ENV SHELL=/bin/bash
ENV HOME=/
RUN apk add bash poetry build-base
WORKDIR /app
ADD pyproject.toml poetry.lock /app/
RUN mkdir /pypoetry
ENV POETRY_VIRTUALENVS_PATH=/pypoetry/cache/virtualenv
ENV SHELL=/bin/bash
ENV HOME=/
RUN poetry install --no-root
ADD . /app
RUN poetry install
SHELL ["/bin/bash", "-c"]
# The redirect to /dev/null seems to help shellingham detect bash!
RUN poetry run datafaker --install-completion > /dev/null
WORKDIR /data
CMD ["bash", "-c", "source $(poetry -C /app env info --path)/bin/activate;bash"]
