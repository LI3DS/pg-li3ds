PIP_COMMAND ?= pip2
PIP_EDITABLE_MODE ?= FALSE
PIP_INSTALL_OPTIONS = --upgrade
ifeq ($(PIP_EDITABLE_MODE), TRUE)
	PIP_INSTALL_OPTIONS += -e
endif

extdir=`pg_config --sharedir`/extension

.PHONY: install
install: install_extension install_python_package

.PHONY: install_extension
install_extension:
	install -m 0644 extension/li3ds.control $(extdir)
	install -m 0644 extension/li3ds--1.0.0.sql $(extdir)

.PHONY: install_python_package
install_python_package:
	$(PIP_COMMAND) install $(PIP_INSTALL_OPTIONS) ./python
