.PHONY: tidy test race vet lint check up down logs fmt smoke

tidy:
	go mod tidy

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

lint:
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"

check: fmt test vet lint

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

smoke:
	bash scripts/smoke.sh
