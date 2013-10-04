.PHONY: clean


test:
	trial norm && pyflakes norm

clean:
	find . -name "*.pyc" -exec rm {} \;

# test on vagrant
vtest:
	-psql -c 'drop database foo;' -U vagrant postgres
	psql -c 'create database foo;' -U vagrant postgres
	trial norm
	
