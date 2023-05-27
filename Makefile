install:
	pip3 install --use-pep517 .

install-dev:
	pip3 install --use-pep517 --editable .

uninstall:
	pip3 uninstall --yes line-protocol-cache-consumer

clean:
	rm -rf *.egg-info build

docker-image:
	docker build --pull --no-cache --tag line-protocol-cache-consumer .
