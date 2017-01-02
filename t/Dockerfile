# DOCKER_NAME=rate-o-mat-functional-tests-jessie
FROM docker.mgm.sipwise.com/sipwise-jessie:latest

# Important! Update this no-op ENV variable when this Dockerfile
# is updated with the current date. It will force refresh of all
# of the base images and things like `apt-get update` won't be using
# old cached versions when the Dockerfile is built.
ENV REFRESHED_AT 2016-01-27

# TODO - the release-trunk-jessie is too dynamic yet, though required build deps
RUN echo "deb https://deb.sipwise.com/autobuild/ release-trunk-jessie main" >>/etc/apt/sources.list

RUN apt-get update
RUN apt-get install --assume-yes \
        libboolean-perl \
        libdata-rmap-perl \
        libdatetime-format-iso8601-perl \
        libdatetime-format-strptime-perl \
        libjson-perl \
        libtap-formatter-junit-perl \
        libtap-harness-archive-perl \
        libtext-table-perl \
        libtime-warp-perl \
        libdbd-mysql-perl \
        libexception-class-perl \
        libnetaddr-ip-perl \
        perl-modules \
        mysql-client
RUN apt-get clean

RUN echo "cd /code && ./t/testrunner 1.2.3.4" > /root/.bash_history

# Create files with mode 666 and folders with mode 777 which
# allows to delete the temporary files (aka results) for
# non-root users (like 'jenkins' or 'developer's laptop local user')
RUN echo 'umask 0' >>/root/.bashrc

WORKDIR /code

################################################################################
# Instructions for usage
# ----------------------
# When you want to build the base image from scratch (jump to the next section if you don't want to build yourself!):
# % docker build --no-cache --tag="rate-o-mat-functional-tests-jessie" .
# % docker run --rm -i -t -v $(pwd)/../:/code:rw rate-o-mat-functional-tests-jessie:latest bash
#
# Use the existing docker image:
# % docker pull docker.mgm.sipwise.com/rate-o-mat-functional-tests-jessie
# % docker run --rm -i -t -v $(pwd)/../:/code:rw docker.mgm.sipwise.com/rate-o-mat-functional-tests-jessie:latest bash
#
# Inside docker:
#   cd /code && ./t/testrunner $IP_OF_NGCP_SYSTEM # IP_OF_NGCP_SYSTEM can be e.g. IP of a `vagrant up ce-trunk` system (eth1)
################################################################################
