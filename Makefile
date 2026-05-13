SHELL := bash
.ONESHELL:
.SHELLFLAGS := -ec

.PHONY: help sbt clean compile test-unit test-coverage test-integration-cassandra test-integration-scylla \
        resolve-cassandra-version resolve-scylla-version resolve-scala-version \
        download-cassandra download-scylla install-cassandra-ccm install-scylla-ccm \
        generate-test-matrix lint lint-fix generate-test-certs \
        release-prepare release release-dry-run checkout-one-commit-before

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SCYLLA_VERSION ?= LATEST
GET_VERSION_VERSION := v0.4.5
GET_VERSION_VERSION_NUM := 0.4.5
GET_VERSION_CHECKSUM := 5a59cbf8c063c141c5904c21b5baebc708724a90b363b0804904f664a8485a84
JAVA_OPENS?=-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
JAVA_VERSION_RAW:=$(shell java -version 2>&1 | head -n 1 | sed -E 's/.*"([0-9]+((\.|_)[0-9]+)*)".*/\1/')
JAVA_MAJOR_VERSION:=$(word 1,$(subst ., ,$(JAVA_VERSION_RAW)))
ifeq ($(JAVA_MAJOR_VERSION),1)
	JAVA_MAJOR_VERSION:=8
endif
ifeq ($(shell [ $(JAVA_MAJOR_VERSION) -ge 9 ] && echo yes),yes)
	JAVA_TOOL_OPTIONS?=$(JAVA_OPENS)
else
	JAVA_OPENS:=
	JAVA_TOOL_OPTIONS:=
endif
SBT_BIN?=./sbt/sbt
SCALA_VERSION ?= 2-LATEST
CASSANDRA_VERSION ?= 4-LATEST
CCM_CASSANDRA_REPO ?= github.com/apache/cassandra-ccm
CCM_CASSANDRA_VERSION ?= trunk
CCM_SCYLLA_REPO ?= github.com/scylladb/scylla-ccm
CCM_SCYLLA_VERSION ?= master

# When building with Java 17+, CCM needs a separate JDK to start older Cassandra.
# Set CCM_JAVA_HOME to point to the JDK that CCM should use to start the database:
#   - Cassandra 3.x/4.x: Java 8
#   - Cassandra 5.x: Java 11
# In CI this is handled by .github/workflows/integration-tests.yml.
ifdef CCM_JAVA_HOME
export CCM_JAVA_HOME
endif

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

export PATH := $(MAKEFILE_PATH)/bin:$(PATH)

CASSANDRA_VERSION_FILE=/tmp/cassandra-version-$(CASSANDRA_VERSION).resolved
SCYLLA_VERSION_FILE=/tmp/scylla-version-$(SCYLLA_VERSION).resolved
SCALA_VERSION_FILE=/tmp/scala-version-$(SCALA_VERSION).resolved

SBT_CMD=$(SBT_BIN) ++$${SCALA_VERSION_RESOLVED:-$$(cat "$(SCALA_VERSION_FILE)")}

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

sbt: ## Ensure SBT is installed
	@$(MAKE) sbt-install

.prepare-bin:
	@[[ -d "$(MAKEFILE_PATH)/bin" ]] || mkdir -p "$(MAKEFILE_PATH)/bin"

.prepare-get-version: .prepare-bin
	@if [[ ! -f "$(MAKEFILE_PATH)/bin/get-version" ]]; then
		echo "bin/get-version is not found, installing it"
		URL="https://github.com/scylladb-actions/get-version/releases/download/$(GET_VERSION_VERSION)/get-version_$(GET_VERSION_VERSION_NUM)_linux_amd64v3.zip"
		echo "Downloading from: $$URL"
		for i in 1 2 3; do
			curl -fsSLo /tmp/get-version.zip "$$URL" && break
			echo "Retry $$i: curl failed, retrying..."
			sleep 2
		done
		echo "$(GET_VERSION_CHECKSUM)  /tmp/get-version.zip" | sha256sum -c - || { echo "Checksum verification failed"; exit 1; }
		unzip /tmp/get-version.zip get-version -d "$(MAKEFILE_PATH)/bin" >/dev/null
	fi

.prepare-scylla-ccm:
	@if ccm --help 2>/dev/null 1>&2 &&
		grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 &&
		grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev/null 1>&2; then
		echo "ScyllaDB CCM ${CCM_SCYLLA_VERSION} is already installed"
	else
	  	$(MAKE) install-scylla-ccm
	fi

resolve-cassandra-version: .prepare-get-version ## Resolve Cassandra version string
	@find "${CASSANDRA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1 || true
	if [[ -f "${CASSANDRA_VERSION_FILE}" ]]; then
		echo "Resolved Cassandra ${CASSANDRA_VERSION} to $$(cat ${CASSANDRA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${CASSANDRA_VERSION}" == "5-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 5.LAST.LAST" | tr -d '"')
	elif [[ "${CASSANDRA_VERSION}" == "4-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 4.LAST.LAST" | tr -d '"')
	elif [[ "${CASSANDRA_VERSION}" == "3-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 3.LAST.LAST" | tr -d '"')
	elif echo "${CASSANDRA_VERSION}" | grep -P '^dse-[0-9\.]+'; then
		CASSANDRA_VERSION_RESOLVED=${CASSANDRA_VERSION}
	elif echo "${CASSANDRA_VERSION}" | grep -P '^[0-9]+\.[0-9]+\.[0-9]+'; then
		CASSANDRA_VERSION_RESOLVED=${CASSANDRA_VERSION}
	else
		echo "Unknown Cassandra version name '${CASSANDRA_VERSION}'"
		exit 1
	fi

	if [[ -z "$$CASSANDRA_VERSION_RESOLVED" ]]; then
		echo "Failed to resolve Cassandra ${CASSANDRA_VERSION}"
		exit 1
	fi

	echo "Resolved Cassandra ${CASSANDRA_VERSION} to $$CASSANDRA_VERSION_RESOLVED"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$$CASSANDRA_VERSION_RESOLVED" >>$${GITHUB_OUTPUT}
	fi
	echo "$$CASSANDRA_VERSION_RESOLVED" >${CASSANDRA_VERSION_FILE}

resolve-scylla-version: .prepare-get-version ## Resolve ScyllaDB version string
	@find "${SCYLLA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1 || true
	if [[ -f "${SCYLLA_VERSION_FILE}" ]]; then
		echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $$(cat ${SCYLLA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${SCYLLA_VERSION}" == "LTS-LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.1.LAST" | tr -d '"')
	elif [[ "${SCYLLA_VERSION}" == "LTS-PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST" | tr -d '"')
	elif [[ "${SCYLLA_VERSION}" == "LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST" | tr -d '"')
	elif [[ "${SCYLLA_VERSION}" == "PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST-1" | tr -d '"')
	elif echo "${SCYLLA_VERSION}" | grep -P '^[0-9]+\.[0-9]+\.[0-9]+'; then
		SCYLLA_VERSION_RESOLVED=${SCYLLA_VERSION}
	else
		echo "Unknown ScyllaDB version name '${SCYLLA_VERSION}'"
		exit 1
	fi

	if [[ -z "$$SCYLLA_VERSION_RESOLVED" ]]; then
		echo "Failed to resolve ScyllaDB '${SCYLLA_VERSION}'"
		exit 1
	fi

	echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $$SCYLLA_VERSION_RESOLVED"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$$SCYLLA_VERSION_RESOLVED" >>$${GITHUB_OUTPUT}
	fi
	echo "$$SCYLLA_VERSION_RESOLVED" >${SCYLLA_VERSION_FILE}

.prepare-cassandra-ccm:
	@for i in 1 2 3; do
		pip install --upgrade psutil >/dev/null && break
		echo "Retry $$i: pip install psutil failed, retrying..."
		sleep 2
	done
	if ccm --help 2>/dev/null 1>&2 &&
		grep CASSANDRA "${CCM_CONFIG_DIR}/ccm-type" 2>/dev/null 1>&2 &&
		grep "${CCM_CASSANDRA_VERSION}" "${CCM_CONFIG_DIR}/ccm-version" 2>/dev/null 1>&2; then
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed"
	else
		$(MAKE) install-cassandra-ccm
	fi

install-cassandra-ccm: ## Install Cassandra CCM
	@echo "Installing Cassandra CCM ${CCM_CASSANDRA_VERSION} from ${CCM_CASSANDRA_REPO}"
	for i in 1 2 3; do
		pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}" && break
		echo "Retry $$i: pip install failed, retrying..."
		sleep 2
	done
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

install-scylla-ccm: ## Install ScyllaDB CCM
	@echo "Installing ScyllaDB CCM ${CCM_SCYLLA_VERSION} from ${CCM_SCYLLA_REPO}"
	for i in 1 2 3; do
		pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}" && break
		echo "Retry $$i: pip install failed, retrying..."
		sleep 2
	done
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

resolve-scala-version: .prepare-get-version ## Resolve Scala version string
	@find "${SCALA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1 || true
	if [[ -f "${SCALA_VERSION_FILE}" ]]; then
		echo "Resolved Scala ${SCALA_VERSION} to $$(cat ${SCALA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${SCALA_VERSION}" == "2-LATEST" ]]; then
		SCALA_VERSION_RESOLVED=$$(get-version -source github-tag -repo scala/scala -prefix "v" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 2.LAST.LAST" | tr -d '"')
	elif [[ "${SCALA_VERSION}" == "2-PRIOR" ]]; then
		SCALA_VERSION_RESOLVED=$$(get-version -source github-tag -repo scala/scala -prefix "v" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 2.LAST-1.LAST" | tr -d '"')
	elif [[ "${SCALA_VERSION}" == "3-LATEST" ]]; then
		SCALA_VERSION_RESOLVED=$$(get-version -source github-tag -repo scala/scala3 -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 3.LAST.LAST" | tr -d '"')
	elif [[ "${SCALA_VERSION}" == "3-PRIOR" ]]; then
		SCALA_VERSION_RESOLVED=$$(get-version -source github-tag -repo scala/scala3 -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 3.LAST-1.LAST" | tr -d '"')
	elif echo "${SCALA_VERSION}" | grep -P '^[0-9]+\.[0-9]+\.[0-9]+'; then
		SCALA_VERSION_RESOLVED=${SCALA_VERSION}
	else
		echo "Unknown Scala version name '${SCALA_VERSION}'"
		exit 1
	fi

	if [[ -z "$$SCALA_VERSION_RESOLVED" ]]; then
		echo "Failed to resolve Scala ${SCALA_VERSION}"
		exit 1
	fi

	echo "Resolved Scala ${SCALA_VERSION} to $$SCALA_VERSION_RESOLVED"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$$SCALA_VERSION_RESOLVED" >>$${GITHUB_OUTPUT}
	fi
	echo "$$SCALA_VERSION_RESOLVED" >${SCALA_VERSION_FILE}

DATABASES ?= scylla:LATEST,scylla:LTS-LATEST,cassandra:3-LATEST,cassandra:4-LATEST,cassandra:5-LATEST
SCALA_VERSIONS ?= 2-LATEST

generate-test-matrix: ## Generate CI test matrix JSON
	@IFS=',' read -ra scala_arr <<< "$(SCALA_VERSIONS)"
	scala_count=$${#scala_arr[@]}
	matrix='{"include":['
	first=true
	index=0
	IFS=',' read -ra db_pairs <<< "$(DATABASES)"
	for pair in "$${db_pairs[@]}"; do
		db_type="$${pair%%:*}"
		db_version="$${pair#*:}"
		scala_index=$$((index % scala_count))
		scala="$${scala_arr[$$scala_index]}"
		if [ "$$first" = true ]; then
			first=false
		else
			matrix="$${matrix},"
		fi
		matrix="$${matrix}{\"db-type\":\"$$db_type\",\"db-version\":\"$$db_version\",\"scala\":\"$$scala\"}"
		index=$$((index + 1))
	done
	matrix="$${matrix}]}"
	echo "$$matrix"
	if [[ -n "$${GITHUB_OUTPUT}" ]]; then
		echo "matrix=$$matrix" >>$${GITHUB_OUTPUT}
	fi

download-cassandra: .prepare-cassandra-ccm resolve-cassandra-version ## Pre-download Cassandra via CCM
	@CASSANDRA_VERSION_RESOLVED=$${CASSANDRA_VERSION_RESOLVED:-$$(cat "${CASSANDRA_VERSION_FILE}")}
	rm -rf /tmp/download.ccm || true
	mkdir -p /tmp/download.ccm
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$$CASSANDRA_VERSION_RESOLVED" --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

download-scylla: .prepare-scylla-ccm resolve-scylla-version ## Pre-download ScyllaDB via CCM
	@SCYLLA_VERSION_RESOLVED=$${SCYLLA_VERSION_RESOLVED:-$$(cat "${SCYLLA_VERSION_FILE}")}
	if [[ "$$SCYLLA_VERSION_RESOLVED" =~ ^[0-9]{4}\. ]]; then
		SCYLLA_VERSION_RESOLVED="release:$$SCYLLA_VERSION_RESOLVED"
	fi
	rm -rf /tmp/download.ccm || true
	mkdir -p /tmp/download.ccm
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$$SCYLLA_VERSION_RESOLVED" --scylla --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

test-integration-cassandra: resolve-scala-version resolve-cassandra-version generate-test-certs ## Run integration tests against Cassandra
	@CASSANDRA_VERSION_RESOLVED=$${CASSANDRA_VERSION_RESOLVED:-$$(cat "${CASSANDRA_VERSION_FILE}")}
	JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" CCM_CASSANDRA_VERSION="$$CASSANDRA_VERSION_RESOLVED" $(SBT_CMD) test it:test

test-integration-scylla: resolve-scala-version resolve-scylla-version generate-test-certs ## Run integration tests against ScyllaDB
	@SCYLLA_VERSION_RESOLVED=$${SCYLLA_VERSION_RESOLVED:-$$(cat "${SCYLLA_VERSION_FILE}")}
	if [[ "$$SCYLLA_VERSION_RESOLVED" =~ ^[0-9]{4}\. ]]; then
		SCYLLA_VERSION_RESOLVED="release:$$SCYLLA_VERSION_RESOLVED"
	fi
	JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" CCM_CASSANDRA_VERSION="$$SCYLLA_VERSION_RESOLVED" CCM_IS_SCYLLA=true $(SBT_CMD) test it:test

compile: resolve-scala-version ## Compile all modules
	@JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" $(SBT_CMD) compile Test/compile IntegrationTest/compile

test-coverage: resolve-scala-version ## Run unit tests with coverage
	@COVERAGE=true JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" $(SBT_CMD) coverage test coverageReport

test-unit: resolve-scala-version ## Run unit tests
	@JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" $(SBT_CMD) test

lint: resolve-scala-version ## Check code with scalafix and scalastyle
	@JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" $(SBT_CMD) "scalafix --check" "Test/scalafix --check" "IntegrationTest/scalafix --check" scalastyle Test/scalastyle

lint-fix: resolve-scala-version ## Auto-fix scalafix issues
	@JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" $(SBT_CMD) scalafix Test/scalafix IntegrationTest/scalafix

TLS_CERT_DIR := $(MAKEFILE_PATH)/test-support/src/main/resources/tls

generate-test-certs: ## Generate TLS test certificates
	@$(MAKEFILE_PATH)/scripts/generate-test-certs.sh "$(TLS_CERT_DIR)"

clean: ## Clean build artifacts and TLS certs
	@$(SBT_BIN) clean
	@rm -rf "$(TLS_CERT_DIR)"

# Release variables
RELEASE_SKIP_TESTS ?=
RELEASE_TARGET_TAG ?=
RELEASE_LOG_DIR := /tmp/spark-connector-release-logs

.require-release-env:
	@if [[ -z "${SONATYPE_USERNAME}" ]]; then
		echo "SONATYPE_USERNAME is empty"
		exit 1
	fi
	if [[ -z "${SONATYPE_PASSWORD}" ]]; then
		echo "SONATYPE_PASSWORD is empty"
		exit 1
	fi

release-prepare: ## Prepare release (version bump + tag)
	@CURRENT_VERSION=$$(grep -oP '(?<=:= ")[^"]+' version.sbt)
	if [[ ! "$$CURRENT_VERSION" =~ -SNAPSHOT$$ ]]; then
		echo "Current version $$CURRENT_VERSION is not a SNAPSHOT version"
		exit 1
	fi
	RELEASE_VERSION=$${CURRENT_VERSION%-SNAPSHOT}
	echo "Preparing release version $$RELEASE_VERSION"
	echo 'ThisBuild / version := "'"$$RELEASE_VERSION"'"' > version.sbt
	git add version.sbt
	git commit -m "[release] prepare release $$RELEASE_VERSION"
	git tag -a "v$$RELEASE_VERSION" -m "Release $$RELEASE_VERSION"
	IFS='.' read -ra parts <<< "$$RELEASE_VERSION"
	NEXT_PATCH=$$(( $${parts[2]} + 1 ))
	NEXT_VERSION="$${parts[0]}.$${parts[1]}.$$NEXT_PATCH-SNAPSHOT"
	echo 'ThisBuild / version := "'"$$NEXT_VERSION"'"' > version.sbt
	git add version.sbt
	git commit -m "[release] prepare for next development iteration"
	echo "Release $$RELEASE_VERSION prepared. Next development version: $$NEXT_VERSION"

release: .require-release-env ## Publish release to Maven Central
	@RELEASE_TAG=$$(git describe --tags --abbrev=0 --match 'v*')
	echo "Performing release for tag $$RELEASE_TAG"
	git checkout "$$RELEASE_TAG"
	mkdir -p "$(RELEASE_LOG_DIR)"
	SBT_CMDS="+publishSigned sonaUpload sonaRelease"
	if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		SBT_CMDS="\"set ThisBuild / test := {}\" $$SBT_CMDS"
	fi
	eval $(SBT_BIN) $$SBT_CMDS > >(tee $(RELEASE_LOG_DIR)/stdout.log) 2> >(tee $(RELEASE_LOG_DIR)/stderr.log)

release-dry-run: .require-release-env ## Dry-run release (no Maven Central publish)
	@RELEASE_TAG=$$(git describe --tags --abbrev=0 --match 'v*')
	echo "Performing dry-run release for tag $$RELEASE_TAG"
	git checkout "$$RELEASE_TAG"
	mkdir -p "$(RELEASE_LOG_DIR)"
	SBT_CMDS="+publishSigned sonaUpload"
	if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		SBT_CMDS="\"set ThisBuild / test := {}\" $$SBT_CMDS"
	fi
	eval $(SBT_BIN) $$SBT_CMDS > >(tee $(RELEASE_LOG_DIR)/stdout.log) 2> >(tee $(RELEASE_LOG_DIR)/stderr.log)

checkout-one-commit-before:
	@if [[ "${RELEASE_TARGET_TAG}" == v* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
	fi
