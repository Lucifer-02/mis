run:
	# export LD_LIBRARY_PATH=/home/lucifer/Downloads/instantclient-basic-linux.x64-19.27.0.0.0dbru/instantclient_19_27/
	# uv run cqn.py
	# uv run main.py
	# uv run test.py
	# uv run test_cqn.py
	# uv run ./poll.py
	pytest ./functional_test_onelake.py
