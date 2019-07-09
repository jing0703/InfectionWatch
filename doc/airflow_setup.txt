# Create a separate m4.large instance for this, with Ubuntu 18.04 LTS AMI.
# Use PostgreSQL instead of the default SQLite. When you get to the section "Let’s create Postgres user for airflow. Still in psql console." retype the psql command instead of copy-and-paste, with single apostrophe in the first line.
# For installing airflow use this command instead: pip install "apache-airflow[ssh, postgres, crypto]"
#  sudo apt-get install python-pip
#  sudo apt-get update
#  sudo apt-get install python-pip
#  sudo pip install --upgrade pip
#  sudo apt-get install postgresql postgresql-contrib
#  sudo adduser airflow
#  sudo usermod -aG sudo airflow
#  su - airflow
# sudo -u postgres psql
# For set up airflow.cfg change the executor to LocalExecutor instead of CeleryExecutor or SequentialExecutor.
# sudo nano /etc/postgresql/9.5/main/pg_hba.conf
# sudo service postgresql restart
# sudo service postgresql restart
# export AIRFLOW_HOME=~/airflow
# pip install "[airflow[postgres, mssql, celery, rabbitmq]" --user
# airflow initdb
# You'll create another user called airflow. Whenever you log in, ensure you use su - airflow to change your account from ubuntu to airflow.
# Create a directory dags under ~/airflow/ and put your DAGs in there.
# Run airflow webserver & and airflow scheduler & and leave 'em open. You might want to daemonize this with -D or nohup.
