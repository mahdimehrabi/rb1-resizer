FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go mod download -x
RUN go mod verify

COPY . .

RUN go build -v -o ./app ./cmd


FROM alpine
COPY --from=builder /app/app .
COPY --from=builder /app/.env .

CMD ["./app"]
