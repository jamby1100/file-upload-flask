# ecv-jpm-file-upload-flask

## Deployment to AWS

### [A] Provision the EC2

- Provision 2 EC2 instances, behind an ALB

### [B] Configure the EC2

```sh
# https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-uwsgi-and-nginx-on-centos-7
sudo yum install python-pip python-devel gcc nginx
```

### [C] Install the app

```sh
git clone https://github.com/jamby1100/file-upload-flask
cd file-upload-flask

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

export UPLOAD_DIRECTORY='/tmp'
export MONGODB_DB_CONNECTION_URI=mongodb://localhost:27017/
export MONGODB_DB_NAME=ecv-jmp-file-upload-app
export ENV_MODE=frontend
export POSTGRESQL_DB_USERNAME=flask_photo_app_admin
export POSTGRESQL_DB_PASSWORD='sac2c2qec1131DSq@#1'
export POSTGRESQL_DB_DATABASE_NAME=ecv_file_upload_app_psql
export POSTGRESQL_DB_HOST=localhost


# run the app!
flask --app main run
flask --app main.py run
# use this when in ec2
flask --app main run --host 0.0.0.0
```

At this stage, you should be able to see hello world at the homepage.
But the other pages would error.

### [D] Provision needed AWS Services

1. AWS RDS
2. AWS DocumentDB
3. EFS

Once RDS is installed, you have to run this command to create the table

```sh
python db/postgresql/init_db.py
```

And ofcourse, you need a fresh set of environment variables. Ofcourse, change according to the new values from your newly provisioned resources:

```sh
export UPLOAD_DIRECTORY='/tmp'
export MONGODB_DB_CONNECTION_URI=mongodb://localhost:27017/
export MONGODB_DB_NAME=ecv-jmp-file-upload-app
export ENV_MODE=frontend
export POSTGRESQL_DB_USERNAME=flask_photo_app_admin
export POSTGRESQL_DB_PASSWORD='sac2c2qec1131DSq@#1'
export POSTGRESQL_DB_DATABASE_NAME=ecv_file_upload_app_psql
export POSTGRESQL_DB_HOST=localhost
```

Hint: You can use tmux to leave the web server running. I know there's a better practice to do this, but this is much faster.

```sh
sudo yum install tmux
# create a session
tmux

# list the active sessions
tmux ls

# reconnect to existing session
tmux attach -t 0
```

### [E] Mount the EFS

```sh
# create the efs
# https://ap-southeast-1.console.aws.amazon.com/efs/home?region=ap-southeast-1#/file-systems/fs-0ab4558735bb03da5?tabId=mounts

# check if installed
sudo yum list installed | grep amazon-efs-utils

# install the tool
sudo yum -y install amazon-efs-utils

# create the damn folder
# https://docs.aws.amazon.com/efs/latest/ug/mounting-fs-mount-helper-ec2-linux.html
sudo mkdir efs

# mount the damn thing
sudo mount -t efs -o tls fs-0ab4558735bb03da5:/ efs

sudo chown -R $USER ~/efs
sudo chgrp -R $USER ~/efs

# ensure port 22 and 2049


# check if mounted already
df -h
```

### [F] Test the app

- visit `http://127.0.0.1:5000/images`


### [G] Resources

- The app is contained almost exclusively on main.py
- Need more context on Flask? Visit `https://flask.palletsprojects.com/en/stable/quickstart/`

## Jamby's corner

```sh
pip install Flask
pip install pymongo
pip install Jinja2
pip install psycopg2-binary
pip freeze > requirements.txt

brew tap mongodb/brew
brew update

# https://www.digitalocean.com/community/tutorials/how-to-use-a-postgresql-database-in-a-flask-application
brew install postgresql@16
brew services start postgresql@16

# setup your postgresql database
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> /Users/raphael.jambalos/.zshrc
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"

psql postgres
CREATE ROLE flask_photo_app_admin WITH LOGIN SUPERUSER PASSWORD 'sac2c2qec1131DSq@#1';
CREATE DATABASE ecv_file_upload_app_psql;

# show databases
\l

# use this db
\c ecv_file_upload_app_psql;

# test
select * from stock_movements;
select * from products;

# https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-os-x/
brew install mongodb-community@8.0
brew services start mongodb-community@8.0

git remote add github_origin git@github.com-personal:jamby1100/file-upload-flask.git 
git push github_origin main
```

## Helpful Docs

https://flask.palletsprojects.com/en/stable/quickstart/
http://127.0.0.1:5000/images

## Installation on EC2

```sh

git clone https://github.com/jamby1100/file-upload-flask.git
cd file-upload-flask/

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt


# run python
`python3 main.py`

# celery server
celery -A main.celery worker --loglevel=info

# https://medium.com/@zghanem/troubleshooting-efs-mounting-volume-failed-mount-unknown-filesystem-type-efs-in-ecs-tasks-6de59137a653


# kafkha


# ufw
sudo apt-get update
sudo apt-get install ufw

# check mongodb creds
echo $MONGODB_DB_NAME
echo $MONGODB_DB_CONNECTION_URI

```

## References

https://www.digitalocean.com/community/tutorials/how-to-use-a-postgresql-database-in-a-flask-application