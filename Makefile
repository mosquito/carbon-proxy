all: build

build: image
	docker build -t mosquito/carbon-proxy:server --target carbon-proxy-server .
	docker build -t mosquito/carbon-proxy:client --target carbon-proxy .

push: build
	docker push mosquito/carbon-proxy:server
	docker push mosquito/carbon-proxy:client

image: sdist
	docker build -t carbon-proxy-base -f Dockerfile .

sdist:
	python3.6 setup.py sdist
