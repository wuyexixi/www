FROM golang:1.13

WORKDIR /
COPY main .

CMD ["/main"]
