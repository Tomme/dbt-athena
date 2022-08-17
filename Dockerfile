FROM python:3.8.10

# Set up build environment
ENV BUILD_DIR /tmp/build
RUN mkdir -p ${BUILD_DIR}
COPY ./ ${BUILD_DIR}

# Install test dependencies
RUN cd ${BUILD_DIR} && make install_deps

# Install dbt-athena-adapter
RUN cd ${BUILD_DIR} && python setup.py install

CMD ["tail", "-f", "/dev/null"]
