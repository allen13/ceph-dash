FROM centos:centos7

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY ceph.repo /etc/yum.repos.d/ceph.repo

RUN yum install -y epel-release && \
    yum install -y python python-pip python-flask && \
    yum install -y ceph && \
    yum clean all -y

RUN curl -L https://github.com/openshift/origin/releases/download/v1.1.6/openshift-origin-client-tools-v1.1.6-ef1caba-linux-64bit.tar.gz | tar xz && \
    mv openshift*/oc /usr/local/bin && \
    rm -rf openshift-origin-client-tools-*

EXPOSE 5000

COPY . /usr/src/app

CMD python ceph-dash.py
