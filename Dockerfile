FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ha-controller ./cmd/controller

FROM alpine:3.20
RUN adduser -D -u 10001 ha
USER ha
COPY --from=build /out/ha-controller /usr/local/bin/ha-controller
ENTRYPOINT ["/usr/local/bin/ha-controller"]
