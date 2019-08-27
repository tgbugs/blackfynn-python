# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS  =
SPHINXBUILD = python -msphinx -W
SPHINXPROJ  = Blackfynn
SOURCEDIR   = .
BUILDDIR    = _build
BF_VERSION  = $(shell python -c 'from __future__ import print_function; import blackfynn; print(blackfynn.__version__)')

# default
.PHONY: clean html
.DEFAULT_GOAL := all

install:
	pip install -r requirements.txt

html:
	$(SPHINXBUILD) $(SOURCEDIR) -b html "$(BUILDDIR)/html/$(BF_VERSION)"

html-latest:
	rm -rf $(BUILDDIR)/html/latest; cp -r $(BUILDDIR)/html/$(BF_VERSION) $(BUILDDIR)/html/latest

clean:
	rm -rf $(BUILDDIR)/*

all: clean html
