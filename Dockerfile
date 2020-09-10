FROM golang:1.15

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/dalga ./cmd/dalga

FROM alpine:latest
ARG GO_PROJECT
RUN touch /etc/dalga.toml
COPY --from=0 /go/bin/dalga /bin/dalga
ENTRYPOINT ["/bin/dalga", "-config", "/etc/dalga.toml"]
