all: build

build: image
	docker build -t mosquito/carbon-proxy:server -f Dockerfile.server .
	docker build -t mosquito/carbon-proxy:client -f Dockerfile.client .

image: sdist
	docker build -t carbon-proxy-base -f Dockerfile .

sdist:
	python3.6 setup.py sdist
