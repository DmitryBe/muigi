REPO=docker-dev.hli.io/ccm/luigi-mesos-test
TAG=0.0.2

run-local:
	python -m luigi --module apps.luigi_test_pipe RootTaskTest --total-tasks 100 --local-scheduler --workers=50

luigid:
	luigid

docker-build:
	docker build -t $(REPO):$(TAG) .

docker-push:
	docker push $(REPO):$(TAG)

