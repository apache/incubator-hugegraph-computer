FROM golang:1.18-alpine AS builder
COPY ./ /src/
WORKDIR /src/
ENV CGO_ENABLED="0"
RUN go build -o /go/bin/app

FROM alpine
EXPOSE 8080
COPY --from=builder /go/bin/app /go/bin/app
COPY --from=builder /src/config/ /go/bin/config/
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /
ENV TZ=Asia/Shanghai
ENV ZONEINFO=/zoneinfo.zip

WORKDIR /go/bin/
ENTRYPOINT ["/go/bin/app"]
