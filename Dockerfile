ARG SRCDIR=/go/src/github.com/omarkhd/litess


# Building stage.
FROM golang:1.15.6 AS build
ARG SRCDIR

WORKDIR ${SRCDIR}
ADD cmd cmd
ADD db db
ADD metrics metrics
ADD server server
ADD go.mod go.sum ./

WORKDIR ${SRCDIR}/cmd/worker
RUN go build -o worker .


# Runtime stage.
FROM golang:1.15.6
ARG SRCDIR

RUN apt-get update && apt-get install -y sqlite3

WORKDIR /opt/litess
COPY --from=build ${SRCDIR}/cmd/worker/worker .
