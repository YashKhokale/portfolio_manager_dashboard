FROM apache/airflow:2.8.3
# Set the user to root
USER root
# Set the working directory in the container
WORKDIR /opt/airflow
# Run the pwd command to print the current working directory
COPY requirements.txt /opt/airflow/dags
# Install py dependencies
RUN sudo pip install --no-cache-dir -r /opt/airflow/dags/requirements.txt
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
# Run multiple commands chained together
RUN sh -c 'sudo apt update && \
           sudo apt install wget && \
           wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
           sudo dpkg -i google-chrome-stable_current_amd64.deb \
            || echo "Installation failed. Continuing with cleanup..." && \
           sudo apt-get install -y -f && \
           google-chrome --version && \
           wget https://storage.googleapis.com/chrome-for-testing-public/123.0.6312.105/linux64/chrome-linux64.zip && \
           sudo apt install unzip && \
           unzip chrome-linux64.zip && \
           sudo mv chrome-linux64 /usr/bin/chromedriver && \
           sudo chown root:root /usr/bin/chromedriver && \
           sudo chmod +x /usr/bin/chromedriver'
# install postgres provider for airflow
RUN sudo pip install apache-airflow-providers-postgres

# install git
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*
# install vim
# RUN sh -c 'sudo apt install -y vim'
# get repo cloned
RUN git clone https://github.com/YashKhokale/portfolio_manager_dashboard.git


