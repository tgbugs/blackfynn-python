FROM ruimashita/numpy

# only copy in necessary files
ADD blackfynn /tmp/install/blackfynn
ADD setup.py  /tmp/install/setup.py
ADD README.md /tmp/install/README.md

# install blackfynn
RUN cd /tmp/install \
    && python setup.py install
