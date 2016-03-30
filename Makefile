check: build
	@python test.py

check-quick: build
	@python test.py --quick

cclean:
	@rm -rf test/test test/benchmark test/*.dSYM \
		 .instrumental.cov *.gcda *.gcno *.gcov

clean: cclean
	@rm -rf gauged/*.pyc gauged/*/*.pyc test/*.pyc *.pyc \
		 dist build gauged.egg-info .coverage htmlcov MANIFEST \

coverage: build
	@coverage run test.py && coverage report -m

coverage-html: coverage
	@coverage html && open htmlcov/index.html

build:
	@if ! find build -name 'libgauged.so' 2>/dev/null | grep '.*' >/dev/null; then \
		python setup.py build; \
	fi

install:
	@python setup.py install

ctest_deps: cclean
	@$(CC) -Iinclude -Itest -O0 -g -std=c99 -pedantic -Wall -Wextra -lm -msse2 \
		-pthread -fprofile-arcs -ftest-coverage $(CFLAGS) \
		lib/*.c test/test.c -o test/test

ctest: ctest_deps
	@test/test

ccoverage: ctest
	@(gcov lib/*.c && cat *.gcov) | less

cformat:
	@clang-format -i **/*.{h,c}

benchmark: build
	@python benchmark.py

cbenchmark: cclean
	@$(CC) -Iinclude -Itest -O3 -std=c99 -pedantic -Wall -Wextra -lm -msse2 \
		-pthread -D_SVID_SOURCE $(CFLAGS) \
		lib/*.c test/benchmark.c -o test/benchmark \
			&& test/benchmark

lint:
	@flake8 *.py gauged test --exclude=__init__.py
	@pylint --rcfile=.pylintrc --output-format=colorized --reports=n gauged

publish:
	@python setup.py sdist upload

test: check

.PHONY: build clean install lint ctest_deps publish
