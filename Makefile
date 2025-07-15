venv ?= .venv

.PHONY: default clean test publish
.SUFFIXES:
.SECONDARY:

################################################################################
## Special Make Targets
################################################################################

default: dist

clean:
	rm -rf dist pystorm.egg-info $(venv)

test: $(venv)
	( . $(venv)/bin/activate && pytest )

publish: dist
	( \
		. $(venv)/bin/activate && twine upload \
		--verbose \
		--repository-url 'https://pypi.cogtree.com/cogtree/test/' \
		dist/* \
	)

################################################################################
## Physical Targets
################################################################################

$(venv): requirements.txt test-requirements.txt setup.cfg
	# Create the virtual env that we are going to install into
	uv venv $(venv)
	# Install package deps
	( . $(venv)/bin/activate && uv pip install -r requirements.txt )
	# Install test deps
	( . $(venv)/bin/activate && uv pip install -r test-requirements.txt )
	# Install build deps
	( . $(venv)/bin/activate && uv pip install build twine)
	############################################################
	# Development Environment Created                          #
	#                                                          #
	# Please use source to activate it for your shell.         #
	############################################################
	@echo "\n\033[1m\033[34msource $(venv)/bin/activate\033[39m\033[0m\n"
	############################################################

dist: $(venv) pystorm/*.py pystorm/serializers/*.py
	( . $(venv)/bin/activate && python -m build --no-isolation )
