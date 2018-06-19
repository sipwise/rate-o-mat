# DOCKER_NAME=rate-o-mat-functional-tests-jessie
FROM docker.mgm.sipwise.com/sipwise-jessie:latest

# Important! Update this no-op ENV variable when this Dockerfile
# is updated with the current date. It will force refresh of all
# of the base images and things like `apt-get update` won't be using
# old cached versions when the Dockerfile is built.
ENV REFRESHED_AT 2018-04-03

# files that get-code generates
COPY t/sources.list.d/builddeps.list /etc/apt/sources.list.d/
COPY t/sources.list.d/preferences /etc/apt/preferences.d/

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

RUN echo "cd /code && ./testrunner 1.2.3.4" > /root/.bash_history

WORKDIR /code

################################################################################
# Instructions for usage
# ----------------------
# When you want to build the base image from scratch
# (jump to the next section if you don't want to build yourself!):
#
# you need to put the proper NGCP sources at t/sources.list.d/builddeps.list
# for instance, release mr4.5:
# echo "deb https://deb.sipwise.com/autobuild/release/release-mr4.5/ release-mr4.5 main" > /
#  t/sources.list.d/builddeps.list
#
# NOTE: run the following command from root folder of git repository:
# % docker build --no-cache --tag="rate-o-mat-functional-tests-jessie" -f t/Dockerfile .
# % docker run --rm -i -t -v $(pwd):/code:rw rate-o-mat-functional-tests-jessie:latest bash
#
# Use the existing docker image:
# % docker pull docker.mgm.sipwise.com/rate-o-mat-functional-tests-jessie
# NOTE: run the following command from root folder of git repository:
# % docker run --rm -i -t -v $(pwd):/code:rw docker.mgm.sipwise.com/rate-o-mat-functional-tests-jessie:latest bash
#
# Inside docker:
#   cd /code && ./t/testrunner $IP_OF_NGCP_SYSTEM # IP_OF_NGCP_SYSTEM can be e.g. IP of a `vagrant up ce-trunk` system (eth1)
################################################################################
