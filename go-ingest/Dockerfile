# Multi-stage build using Chainguard images for minimal, non-root runtime.
FROM cgr.dev/chainguard/go:latest AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/ingest .

FROM cgr.dev/chainguard/static:latest
WORKDIR /app
COPY --from=build /out/ingest /app/ingest
USER 65532
ENTRYPOINT ["/app/ingest"]
