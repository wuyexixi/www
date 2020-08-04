FROM golang:1.13

WORKDIR /go/src/www
COPY . .

RUN go get -d -v ./... && \
    go build main.go && \
    cp main / && \
    rm -rf /go/src

CMD ["/main"]
