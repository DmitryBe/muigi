
run-local:
	python -m luigi --module apps.luigi_test_pipe RootTaskTest --local-scheduler --n 10 --workers=10

luigid:
	luigid
