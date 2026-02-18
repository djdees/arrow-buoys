# Chainguard Python image — non-root, minimal attack surface.
FROM cgr.dev/chainguard/python:latest
WORKDIR /app

# Install runtime deps only (local dev uses uv outside the container)
ENV PIP_NO_CACHE_DIR=1
RUN python -m pip install --upgrade pip \
 && python -m pip install \
      "pyarrow>=14.0.0" \
      "polars>=0.20.0" \
      "requests>=2.31.0" \
      "matplotlib>=3.8.0"

COPY main.py /app/main.py

ENV SOURCE_URL=http://go-source:8080/stream
ENV OUT_DIR=/app/out

CMD ["python", "/app/main.py"]
