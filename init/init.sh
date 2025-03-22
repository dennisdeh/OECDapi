echo " ### Setting user privileges ###"

# set permissions for user:pass, then flush to apply
mariadb -uroot -p$MYSQL_ROOT_PASSWORD --execute \
"GRANT ALL PRIVILEGES ON standard.* TO '$MARIADB_USER'@'%' IDENTIFIED BY '$MARIADB_PASSWORD';
FLUSH PRIVILEGES;"
