distribute:
	pip install --upgrade pip build twine
	rm -rf dist
	python -m build
	python -m twine upload dist/*