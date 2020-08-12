#!/bin/bash

# Install hadoop
# Installation relies on finding JAVA_HOME@/usr/java/latest as a prerequisite

INSTALL_DIR=${INSTALL_DIR:-/usr}
USER=`whoami`

HADOOP=hadoop-${HADOOP_VER:-3.2.1}
HADOOP_DIR=${INSTALL_DIR}/$HADOOP

install_prereqs() {
  if [[ ! -d /usr/java ]]; then
      sudo mkdir /usr/java
  fi
  if [[ -f /usr/java/latest ]]; then
      sudo rm /usr/java/latest
  fi
#  sudo apt update &&
  sudo apt install openjdk-8-jre-headless &&
  sudo ln -s /usr/lib/jvm/java-1.8.0-openjdk-amd64/ /usr/java/latest &&
  echo "install_prereqs successful"
}

download_gcs_connector() {
  if [[ $INSTALL_TYPE == gcs ]]; then
    wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
	mv gcs-connector-latest-hadoop2.jar ${HADOOP_DIR}/share/hadoop/common
	echo "Listing ${HADOOP_DIR}/share/hadoop/common"
	ls -l ${HADOOP_DIR}/share/hadoop/common
  fi
}

download_hadoop() {
  wget -q http://www-eu.apache.org/dist/hadoop/common/$HADOOP/$HADOOP.tar.gz &&
  sudo tar -xzvf $HADOOP.tar.gz --directory $INSTALL_DIR &&
  sudo chown -R $USER:$USER $HADOOP_DIR &&
  download_gcs_connector &&
  echo "download_hadoop successful"
}

configure_passphraseless_ssh() {
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa &&
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys &&
  chmod 0600 ~/.ssh/authorized_keys &&
  ssh-keyscan -H localhost >> ~/.ssh/known_hosts &&
  ssh-keyscan -H "0.0.0.0" >> ~/.ssh/known_hosts &&
  echo "configure_passphraseless_ssh successful"
}

configure_hadoop() {
  configure_passphraseless_ssh &&
  cp -fr $TRAVIS_BUILD_DIR/.travis/resources/hadoop/* $HADOOP_DIR/etc/hadoop &&
  mkdir $HADOOP_DIR/logs &&  
  $HADOOP_DIR/bin/hadoop namenode -format &&
  $HADOOP_DIR/sbin/start-dfs.sh &&
  echo "configure_hadoop successful"
}

setup_paths() {
  export PATH=$HADOOP_DIR/bin:$PATH
  export CLASSPATH=`$HADOOP_DIR/bin/hadoop classpath --glob`
  AZURE_JARS=`find $HADOOP_DIR/share/hadoop/tools/lib -name *azure*jar | tr '\n' ':'`
  if [[ ! -z $AZURE_JARS ]]; then
      export CLASSPATH=$AZURE_JARS$CLASSPATH
  fi
  echo $CLASSPATH
  echo "setup_paths successful"
}

install_hadoop() {
  mkdir -p $TRAVIS_BUILD_DIR/scripts
  cd $TRAVIS_BUILD_DIR/scripts &&
  install_prereqs &&
  download_hadoop &&
  setup_paths &&
  configure_hadoop &&
  echo "Install Hadoop SUCCESSFUL"
}

if [[ $INSTALL_TYPE != basic && $TRAVIS_OS_NAME == linux ]]; then
  install_hadoop
fi





