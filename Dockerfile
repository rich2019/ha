FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG APP=controller
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ha-service ./cmd/${APP}

FROM alpine:3.20
RUN addgroup -S -g 10001 ha && adduser -S -D -H -u 10001 -G ha ha
USER ha
COPY --from=build /out/ha-service /usr/local/bin/ha-service
ENTRYPOINT ["/usr/local/bin/ha-service"]
