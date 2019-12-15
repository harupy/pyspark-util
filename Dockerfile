FROM ubuntu:18.04

ARG SPARK_VERSION=2.4.4
ARG HADOOP_VERSION=2.7
ARG SPARK_BIN=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG PYTHON_VERSION=3.6.9
ARG CONDA_ENV_NAME=psu-dev-env

ENV SPARK_HOME /spark
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV MINICONDA_HOME=/miniconda

WORKDIR /pyspark-util

# change default shell
SHELL ["/bin/bash", "-c"]
RUN chsh -s /bin/bash

# update apt and install packages
RUN apt-get -y update && \
    apt-get -y install --no-install-recommends wget git openjdk-8-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# download spark
RUN wget http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/${SPARK_BIN}.tgz \
    && tar -xvzf ${SPARK_BIN}.tgz \
    && mv ${SPARK_BIN} ${SPARK_HOME} \
    && rm ${SPARK_BIN}.tgz

COPY requirements-dev.txt .

# install miniconda and create dev env
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.7.12-Linux-x86_64.sh -O /miniconda.sh \
    && bash /miniconda.sh -b -p $MINICONDA_HOME \
    && . $MINICONDA_HOME/etc/profile.d/conda.sh \
    && conda update conda  --yes \
    && conda create \
    --yes --no-default-packages --channel conda-forge \
    --name $CONDA_ENV_NAME --file requirements-dev.txt python=$PYTHON_VERSION

# create a script to activate dev env
RUN echo ". $MINICONDA_HOME/etc/profile.d/conda.sh" >> /activate_env.sh \
    && echo "conda activate $CONDA_ENV_NAME" >> /activate_env.sh \
    && echo "/activate_env.sh" >> ~/.bashrc

# run activate_env.sh for non-interactive model
ENV BASH_ENV /activate_env.sh

CMD ["/bin/bash"]
