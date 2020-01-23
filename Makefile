all: build

build: sdist
	docker build -t pavkazzz/jaeger-proxy:server --target jaeger-proxy-server .

push: build
	docker push pavkazzz/jaeger-proxy:server

sdist: clean
	python3.7 setup.py sdist

clean:
	rm -rf dist/

lint:
	env/bin/pylama jaeger_proxy tests

test:
	env/bin/pytest \
	--cov jaeger_proxy \
  	--cov-report=term-missing \
  	--doctest-modules \
  	--pylama jaeger_proxy \
  	--pylama tests \
  	tests

develop:
	python3.7 -m venv env
	env/bin/pip install -Ue '.[develop]'
