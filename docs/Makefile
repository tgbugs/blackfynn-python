# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS  =
SPHINXBUILD = python -msphinx
SPHINXPROJ  = Blackfynn
SOURCEDIR   = .
BUILDDIR    = _build
BF_VERSION  = $(shell python -c 'import blackfynn; print blackfynn.__version__')

# default
.PHONY: clean html
.DEFAULT_GOAL := all

html:
	$(SPHINXBUILD) $(SOURCEDIR) -b html "$(BUILDDIR)/html/$(BF_VERSION)"

html-latest:
	cp -r $(BUILDDIR)/html/$(BF_VERSION) $(BUILDDIR)/html/latest

clean:
	rm -rf $(BUILDDIR)/*

all: clean html