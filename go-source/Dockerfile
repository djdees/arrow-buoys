# Multi-stage build using Chainguard images for minimal, non-root runtime.
FROM cgr.dev/chainguard/go:latest AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -buildvcs=false -trimpath -ldflags="-s -w" -o /out/server .

FROM cgr.dev/chainguard/static:latest
WORKDIR /app
COPY --from=build /out/server /app/server
USER 65532
EXPOSE 8080
ENTRYPOINT ["/app/server"]
