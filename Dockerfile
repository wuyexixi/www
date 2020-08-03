FROM golang:1.13

WORKDIR /go/src/www
COPY . .

RUN go get -d -v ./...
RUN go build main.go

CMD ["main"]
