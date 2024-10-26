default: build

test: 
	go test ./...

build: 
	mkdir -p dist
	go build -v -o dist/dump1090reader

# example make run $ARGS="-addr="sub.domain.com"
run: 
	go run -race . $(ARGS)

clean:
	rm -rf dist/
