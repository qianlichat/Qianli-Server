.PHONY: build
build:
	mvn install -DskipTests -Pexclude-spam-filter
