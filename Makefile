build:
	docker build -t omarkhd/litess:latest .

test: build
	docker run -it --rm \
	--volume "$(CURDIR):/go/src/github.com/omarkhd/litess" \
	-w /go/src/github.com/omarkhd/litess \
	omarkhd/litess:latest \
	go test -v -cover -race ./...

fmt: build
	docker run -it --rm \
	--volume "$(CURDIR):/go/src/github.com/omarkhd/litess" \
	-w /go/src/github.com/omarkhd/litess \
	omarkhd/litess:latest \
	go fmt ./...

clean:
	docker rmi -f omarkhd/litess:latest

down:
	docker-compose down

up: build
	docker-compose up --build
