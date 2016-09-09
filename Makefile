REPO=docker-dev.hli.io/ccm/luigi-mesos-test
TAG=0.0.1

run-local:
	python -m luigi --module apps.luigi_test_pipe RootTaskTest --local-scheduler --n 10 --workers=10

luigid:
	luigid

docker-build:
	docker build -t $(REPO):$(TAG) .

docker-push:
	docker push $(REPO):$(TAG)

