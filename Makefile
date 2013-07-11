.PHONY: clean


test:
	trial norm && pyflakes norm

clean:
	find . -name "*.pyc" -exec rm {} \;

