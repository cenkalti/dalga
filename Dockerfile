FROM golang:1.15

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION
ARG COMMIT
ARG DATE
RUN CGO_ENABLED=0 go build -o /go/bin/dalga -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT -X main.date=$DATE" ./cmd/dalga

FROM alpine:latest
RUN touch /etc/dalga.toml
COPY --from=0 /go/bin/dalga /bin/dalga
ENTRYPOINT ["/bin/dalga", "-config", "/etc/dalga.toml"]
