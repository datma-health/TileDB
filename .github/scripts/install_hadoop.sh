#!/bin/bash

# Install hadoop
# Installation relies on finding JAVA_HOME@/usr/java/latest as a prerequisite

INSTALL_DIR=${INSTALL_DIR:-/usr}
USER=`whoami`

HADOOP=hadoop-${HADOOP_VER:-3.2.2}
HADOOP_DIR=${INSTALL_DIR}/$HADOOP
HADOOP_ENV=$HADOOP_DIR/hadoop.env

install_prereqs() {
  if [[ -f /usr/java/latest ]]; then
    echo "/usr/java/latest found"
      sudo rm /usr/java/latest
  fi
  if [[ ! -z $JAVA_HOME ]]; then
    sudo mkdir -p /usr/java
    sudo ln -s $JAVA_HOME /usr/java/latest
  else 
    sudo apt install openjdk-8-jre-headless
    sudo ln -s /usr/lib/jvm/java-1.8.0-openjdk-amd64/ /usr/java/latest
  fi
  echo "install_prereqs successful"
}

# retry logic from: https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-script-actions-linux
MAXATTEMPTS=3
retry() {
    local -r CMD="$@"
    local -i ATTEMPTNUM=1
    local -i RETRYINTERVAL=2

    until $CMD
    do
        if (( ATTEMPTNUM == MAXATTEMPTS ))
        then
                echo "Attempt $ATTEMPTNUM failed. no more attempts left."
                return 1
        else
                echo "Attempt $ATTEMPTNUM failed! Retrying in $RETRYINTERVAL seconds..."
                sleep $(( RETRYINTERVAL ))
                ATTEMPTNUM=$ATTEMPTNUM+1
        fi
    done
}

download_gcs_connector() {
  if [[ $INSTALL_TYPE == gcs ]]; then
    retry wget -nv --trust-server-names https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
	  mv gcs-connector-hadoop3-latest.jar ${HADOOP_DIR}/share/hadoop/common
    echo "download_gcs_connector successful"
  fi
}

download_hadoop() {
  retry wget -nv --trust-server-names https://archive.apache.org/dist/hadoop/common/$HADOOP/$HADOOP.tar.gz
  tar -xzf $HADOOP.tar.gz --directory $INSTALL_DIR &&
  download_gcs_connector &&
  echo "download_hadoop successful"
}

configure_passphraseless_ssh() {
  sudo apt update; sudo apt -y install openssh-server
  cat > sshd_config << EOF
          SyslogFacility AUTHPRIV
          PermitRootLogin yes
          AuthorizedKeysFile	.ssh/authorized_keys
          PasswordAuthentication yes
          ChallengeResponseAuthentication no
          UsePAM yes
          UseDNS no
          X11Forwarding no
          PrintMotd no
EOF
  sudo mv sshd_config /etc/ssh/sshd_config &&
  sudo systemctl restart ssh &&
  ssh-keygen -t rsa -b 4096 -N '' -f ~/.ssh/id_rsa &&
  cat ~/.ssh/id_rsa.pub | tee -a ~/.ssh/authorized_keys &&
  chmod 600 ~/.ssh/authorized_keys &&
  chmod 700 ~/.ssh &&
  sudo chmod -c 0755 ~/ &&
  echo "configure_passphraseless_ssh successful"
}

configure_hadoop() {
  configure_passphraseless_ssh &&
  $HADOOP_DIR/bin/hdfs namenode -format &&
  $HADOOP_DIR/sbin/start-dfs.sh &&
  if [[ $INSTALL_TYPE == gcs ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS=$GITHUB_WORKSPACE/.github/resources/gcs/GCS.json
    source $GITHUB_WORKSPACE/.github/resources/gcs/gcs_cred.sh
  fi
  if [[ $INSTALL_TYPE == azure ]]; then
    source $GITHUB_WORKSPACE/.github/resources/azure/azure_cred.sh
  fi
  echo "configure_hadoop successful"
}

setup_paths() {
  echo "export JAVA_HOME=/usr/java/latest" > $HADOOP_ENV
  echo "export PATH=$HADOOP_DIR/bin:$PATH" >> $HADOOP_ENV
  echo "export LD_LIBRARY_PATH=$HADOOP_DIR/lib:$LD_LIBRARY_PATH" >> $HADOOP_ENV
  HADOOP_CP=`$HADOOP_DIR/bin/hadoop classpath --glob`
  AZURE_JARS=`find $HADOOP_DIR/share/hadoop/tools/lib -name *azure*jar | tr '\n' ':'`
  echo "export CLASSPATH=$AZURE_JARS$HADOOP_CP" >> $HADOOP_ENV
  echo "setup_paths successful"
}

install_hadoop() {
  echo "Installing Hadoop..."
  install_prereqs
  if [[ ! -f $HADOOP_ENV ]]; then
    download_hadoop &&
      setup_paths &&
      cp -fr $GITHUB_WORKSPACE/.github/resources/hadoop/* $HADOOP_DIR/etc/hadoop &&
      mkdir -p $HADOOP_DIR/logs &&
      export HADOOP_ROOT_LOGGER=ERROR,console
  fi
  source $HADOOP_ENV &&
    configure_hadoop &&
    echo "Install Hadoop SUCCESSFUL"
}

echo "INSTALL_DIR=$INSTALL_DIR"
echo "INSTALL_TYPE=$INSTALL_TYPE"
# resources r.tar file encrypted using "gpg --symmetric --cipher-algo AES256 r.tar"
gpg --quiet --batch --yes --decrypt --passphrase="$R_TAR" --output $INSTALL_DIR/r.tar $GITHUB_WORKSPACE/.github/scripts/r.tar.gpg && mkdir $GITHUB_WORKSPACE/yy &&
tar xf $INSTALL_DIR/r.tar -C $GITHUB_WORKSPACE/yy && echo "**** yy" && ls -l $GITHUB_WORKSPACE/yy/resources/azure && echo "**** yy done"
tar xf $INSTALL_DIR/r.tar -C $GITHUB_WORKSPACE/.github &&
install_hadoop

