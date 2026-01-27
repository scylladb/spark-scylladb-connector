SHELL := bash
.ONESHELL:

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SCYLLA_VERSION ?= LATEST
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

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

export PATH := $(MAKEFILE_PATH)/bin:$(PATH)

CASSANDRA_VERSION_FILE=/tmp/cassandra-version-$(CASSANDRA_VERSION).resolved
SCYLLA_VERSION_FILE=/tmp/scylla-version-$(SCYLLA_VERSION).resolved
SCALA_VERSION_FILE=/tmp/scala-version-$(SCALA_VERSION).resolved

SBT_CMD=$(SBT_BIN) ++$${SCALA_VERSION_RESOLVED:-$$(cat "$(SCALA_VERSION_FILE)")}

sbt:
	@$(MAKE) sbt-install

.prepare-bin:
	@[[ -d "$(MAKEFILE_PATH)/bin" ]] || mkdir -p "$(MAKEFILE_PATH)/bin"

.prepare-get-version: .prepare-bin
	@if [[ ! -f "$(MAKEFILE_PATH)/bin/get-version" ]]; then
		echo "bin/get-version is not found, installing it"
		curl -sSLo /tmp/get-version.zip https://github.com/scylladb-actions/get-version/releases/download/v0.3.0/get-version_0.3.0_linux_amd64v3.zip
		unzip /tmp/get-version.zip get-version -d "$(MAKEFILE_PATH)/bin" >/dev/null
	fi

.prepare-scylla-ccm:
	@ccm --help 2>/dev/null 1>&2
	if [[ $$? -lt 127 ]] \
		&& grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 \
		&& grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev/null 1>&2; then
		echo "ScyllaDB CCM ${CCM_SCYLLA_VERSION} is already installed"
	else \
	  	$(MAKE) install-scylla-ccm; \
	fi

resolve-cassandra-version: .prepare-get-version
	@find "${CASSANDRA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
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

resolve-scylla-version: .prepare-get-version
	@find "${SCYLLA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
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
	@pip install --upgrade psutil >/dev/null
	ccm --help 2>/dev/null 1>&2
	if [[ $$? -lt 127 ]]
		&& grep CASSANDRA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2
		&& grep ${CCM_CASSANDRA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev/null 1>&2; then
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed"
	else
	  	$(MAKE) install-cassandra-ccm
	fi

install-cassandra-ccm:
	@echo "Installing Cassandra CCM ${CCM_CASSANDRA_VERSION} from ${CCM_CASSANDRA_REPO}"
	pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

install-scylla-ccm:
	@echo "Installing ScyllaDB CCM ${CCM_SCYLLA_VERSION} from ${CCM_SCYLLA_REPO}"
	pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

resolve-scala-version: .prepare-get-version
	@find "${SCALA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
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

download-cassandra: .prepare-cassandra-ccm resolve-cassandra-version
	@CASSANDRA_VERSION_RESOLVED=$${CASSANDRA_VERSION_RESOLVED:-$$(cat "${CASSANDRA_VERSION_FILE}")}
	rm -rf /tmp/download.ccm || true
	mkdir -p /tmp/download.ccm
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$$CASSANDRA_VERSION_RESOLVED" --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

download-scylla: .prepare-scylla-ccm resolve-scylla-version
	@SCYLLA_VERSION_RESOLVED=$${SCYLLA_VERSION_RESOLVED:-$$(cat "${SCYLLA_VERSION_FILE}")}
	[[ "$$SCYLLA_VERSION_RESOLVED" =~ ^[0-9]{4}\. ]] && SCYLLA_VERSION_RESOLVED="release:$${SCYLLA_VERSION_RESOLVED}"
	rm -rf /tmp/download.ccm || true
	mkdir -p /tmp/download.ccm
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$$SCYLLA_VERSION_RESOLVED" --scylla --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

test-integration-cassandra: resolve-scala-version resolve-cassandra-version
	@CASSANDRA_VERSION_RESOLVED=$${CASSANDRA_VERSION_RESOLVED:-$$(cat "${CASSANDRA_VERSION_FILE}")}
	JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" CCM_CASSANDRA_VERSION="$$CASSANDRA_VERSION_RESOLVED" $(SBT_CMD) test it:test

test-integration-scylla: resolve-scala-version resolve-scylla-version
	@SCYLLA_VERSION_RESOLVED=$${SCYLLA_VERSION_RESOLVED:-$$(cat "${SCYLLA_VERSION_FILE}")}
	[[ "$$SCYLLA_VERSION_RESOLVED" =~ ^[0-9]{4}\. ]] && SCYLLA_VERSION_RESOLVED="release:$${SCYLLA_VERSION_RESOLVED}"
	JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" CCM_CASSANDRA_VERSION="$$SCYLLA_VERSION_RESOLVED" CCM_IS_SCYLLA=true $(SBT_CMD) test it:test

test-unit: resolve-scala-version
	@JAVA_TOOL_OPTIONS="$(JAVA_TOOL_OPTIONS)" $(SBT_CMD) test

clean:
	@$(SBT_BIN) clean
