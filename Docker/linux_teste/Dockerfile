# Usar uma imagem base do Ubuntu
FROM ubuntu:20.04

# Evitar prompts interativos durante a instalação de pacotes
ENV DEBIAN_FRONTEND=noninteractive

# Instalar pacotes necessários
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    vim \
    git \
    openssh-server \
    python3 \
    python3-pip \
    && apt-get clean

# Configurar o SSH
RUN mkdir /var/run/sshd
RUN echo 'root:skate' | chpasswd

# Ajustar as configurações do SSH
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
RUN sed -i 's@session    required     pam_loginuid.so@session    optional     pam_loginuid.so@g' /etc/pam.d/sshd

# Criar diretório .ssh com permissões corretas
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh

# Ajustar permissões das chaves SSH
RUN chmod 600 /etc/ssh/ssh_host_rsa_key /etc/ssh/ssh_host_rsa_key.pub

# Instalar PySpark e python-dotenv
RUN pip3 install pyspark python-dotenv unidecode

# Baixar e instalar Hadoop
ENV HADOOP_VERSION=3.3.6
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
RUN tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/
RUN mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Baixar e instalar Spark
ENV SPARK_VERSION=3.4.3
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
RUN tar -xzvf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/
RUN mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Baixar e mover o driver JDBC do PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P /opt/spark/jars/

# Criar diretório para repositórios e clonar o repositório específico
ENV PROJECT_NAME=weather_traffic_data_flow
RUN mkdir -p /root/repositorios
RUN git clone --branch main https://github.com/eng-niteB/weather_traffic_data_flow.git /root/repositorios/${PROJECT_NAME}/
RUN mkdir -p /root/repositorios/${PROJECT_NAME}/database/raw
RUN mkdir -p /root/repositorios/${PROJECT_NAME}/database/trusted
RUN mkdir -p /root/repositorios/${PROJECT_NAME}/database/refined

# Configurar variáveis de ambiente Java
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Adicionar um script de inicialização para copiar segredos
COPY init.sh /usr/local/bin/init.sh
RUN chmod +x /usr/local/bin/init.sh

# Expor porta para SSH
EXPOSE 22

# Iniciar o SSH e manter o contêiner em execução
CMD ["/usr/local/bin/init.sh", "/usr/sbin/sshd", "-D"]