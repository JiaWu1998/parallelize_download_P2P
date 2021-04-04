help:
	@echo "---------------HELP-----------------"
	@echo "To run evaluation 1, type make eval_1"
	@echo "To run evaluation 2, type make eval_2"
	@echo "To run evaluation 3, type make eval_3"
	@echo "To setup a sample p2p server, type make setup"
	@echo "To clean up a sample p2p server, type make clean"
	@echo "------------------------------------"

eval_1:
	python3 deployment/deployment.py -1

eval_2:
	python3 deployment/deployment.py -2

eval_3:
	python3 deployment/deployment.py -3

setup:
	python3 deployment/deployment.py -c 2

clean:
	python3 deployment/deployment.py -d 2