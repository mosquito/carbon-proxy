all: build

build: image
	docker build -t mosquito/carbon-proxy:server -f Dockerfile.server .
	docker build -t mosquito/carbon-proxy:client -f Dockerfile.client .

push: build
	docker push mosquito/carbon-proxy:server
	docker push mosquito/carbon-proxy:client

image: sdist
	docker build -t carbon-proxy-base -f Dockerfile .

sdist:
	python3.6 setup.py sdist
