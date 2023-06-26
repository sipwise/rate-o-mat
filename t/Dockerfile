# DOCKER_NAME=rate-o-mat-functional-tests-bookworm
FROM docker.mgm.sipwise.com/sipwise-bookworm:latest

# Important! Update this no-op ENV variable when this Dockerfile
# is updated with the current date. It will force refresh of all
# of the base images and things like `apt-get update` won't be using
# old cached versions when the Dockerfile is built.
ENV REFRESHED_AT 2023-06-26

# files that get-code generates
COPY t/sources.list.d/builddeps.list /etc/apt/sources.list.d/
COPY t/sources.list.d/preferences /etc/apt/preferences.d/

RUN apt-get update && \
    apt-get install --assume-yes \
        libboolean-perl \
        libdata-rmap-perl \
        libdatetime-format-iso8601-perl \
        libdatetime-format-strptime-perl \
        libdbd-mysql-perl \
        libexception-class-perl \
        libjson-perl \
        libnetaddr-ip-perl \
        libtap-formatter-junit-perl \
        libtap-harness-archive-perl \
        libtext-table-perl \
        libtime-warp-perl \
        mariadb-client \
        perl \
        && \
    apt-get clean

RUN echo "cd /code && ./t/testrunner 1.2.3.4" > /root/.bash_history

WORKDIR /code

################################################################################
# Instructions for usage
# ----------------------
# When you want to build the base image from scratch
# (jump to the next section if you don't want to build yourself!):
#
# you need to put the proper NGCP sources at t/sources.list.d/builddeps.list
# for instance, trunk:
# echo "deb https://deb.sipwise.com/autobuild/ release-trunk-bookworm main" > t/sources.list.d/builddeps.list
#
# NOTE: run the following command from root folder of git repository:
# % docker build --no-cache --tag="rate-o-mat-functional-tests-bookworm" -f t/Dockerfile .
# % docker run --rm -i -t -v $(pwd):/code:rw rate-o-mat-functional-tests-bookworm:latest bash
#
# Use the existing docker image:
# % docker pull docker.mgm.sipwise.com/rate-o-mat-functional-tests-bookworm
# NOTE: run the following command from root folder of git repository:
# % docker run --rm -i -t -v $(pwd):/code:rw docker.mgm.sipwise.com/rate-o-mat-functional-tests-bookworm:latest bash
#
# Inside docker:
#   cd /code && ./t/testrunner $IP_OF_NGCP_SYSTEM # IP_OF_NGCP_SYSTEM can be e.g. IP of a `vagrant up ce-trunk` system (eth1)
################################################################################
