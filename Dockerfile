# syntax=docker/dockerfile:1

# Build stage
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bidder .

# Runtime stage (non-root, distroless, read-only-friendly)
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bidder /bidder
USER nonroot:nonroot
ENTRYPOINT ["/bidder"]