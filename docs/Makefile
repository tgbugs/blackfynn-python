# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = python -msphinx
SPHINXPROJ    = Blackfynn
SOURCEDIR     = .
BUILDDIR      = _build

# default
.PHONY: clean html
.DEFAULT_GOAL := all

html:
	$(SPHINXBUILD) -d $(BUILDDIR)/doctrees $(SOURCEDIR) -b html "$(BUILDDIR)/html"
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

clean:
	rm -rf $(BUILDDIR)/*

all: clean html