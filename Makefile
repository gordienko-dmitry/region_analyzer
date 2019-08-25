prod:
	echo "Installing python & requirments..."
	-apt-get install -y sudo
	sudo apt-get install -y build-essential
	sudo apt-get install -y python3
	sudo apt-get install -y python3-dev
	sudo apt-get install -y python3-pip
	pip3 install --upgrade pip

	pip3 install -r  requirements.txt
	echo done

	echo "Installing database..."
	sudo apt-get install -y postgresql-10 postgresql-contrib-10
	sudo service postgresql start
	cp ./deployment/pg_hba.conf /etc/postgresql/10/main/hb_hba.conf
	sudo service postgresql restart
	-sudo -u postgres createdb import
	sudo -u postgres psql --command "ALTER USER postgres WITH superuser password 'postgres';"
	echo done

	echo "Configure app..."
	-python3 prepare_db.py
	echo done

	echo "Supervisor"
	-mkdir /var/log/r_analyzer
	pip3 install supervisor
	sudo apt-get -y install supervisor
	sudo service supervisor restart
	cp ./deployment/r_analyzer_sp.conf /etc/supervisor/conf.d/
	sudo supervisorctl reread
	sudo supervisorctl update
	sudo supervisorctl restart r_analyzer:*

	echo "Installing & configure nginx..."
	sudo apt-get install -y nginx
	cp ./deployment/r_analyzer_ng.conf /etc/nginx/conf.d/
	sudo service nginx start
	sudo service nginx restart
