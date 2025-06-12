# Docker instructions to build image of the demo_ofa_app
#
# build image with:
#    docker build -t ivac-tool-monitoring-app .

FROM python:3.13-slim-bullseye

# Build arguments
ARG UNAME=appuser
ARG UID=1200
ARG GID=1200
ARG VERSION=latest
ARG APPLICATION_MANUFACTURER=test_repo
ARG OPENFACTORY_VERSION=latest

LABEL openfactory.version="${OPENFACTORY_VERSION}" \
      application.version="${VERSION}" \
      source.repository="${APPLICATION_MANUFACTURER}"

# Creates a non-root user with an explicit UID
RUN adduser --uid ${UID} --disabled-password --gecos "" ${UNAME}

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Install OpenFactory — if version is "latest", omit @version
RUN if [ "$OPENFACTORY_VERSION" = "latest" ]; then \
      pip install git+https://github.com/Demo-Smart-Factory-Concordia-University/OpenFactory.git ; \
    else \
      pip install git+https://github.com/Demo-Smart-Factory-Concordia-University/OpenFactory.git@$OPENFACTORY_VERSION ; \
    fi

# Set environment variable from build argument
ENV APPLICATION_VERSION=${VERSION}
ENV APPLICATION_MANUFACTURER=${APPLICATION_MANUFACTURER}

WORKDIR /ofa
COPY tool_monitoring.py /ofa
RUN chown -R appuser:appuser /ofa

# Switches to non-root user
USER ${UNAME}

CMD ["python3", "-u", "/ofa/tool_monitoring.py"]