
run-local:
	python -m luigi --module apps.luigi_test_pipe RootTaskTest --local-scheduler --workers=10

luigid:
	luigid
