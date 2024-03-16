# Linux VM Commands

## De basisconfiguratie
- ssh -p 40197 vicuser@vichogent.be

## Microsoft SQL Server

### Install SQL Server
- sudo curl -o /etc/yum.repos.d/mssql-server.repo https://packages.microsoft.com/config/rhel/9/mssql-server-2022.repo
- sudo yum install -y mssql-server
- sudo /opt/mssql/bin/mssql-conf setup (pswd = VMdepgroup30)
- systemctl status mssql-server
- sudo firewall-cmd --zone=public --add-port=1433/tcp --permanent
- sudo firewall-cmd --reload

## Install the SQL Server command-line tools
- curl https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo (Red Hat 9- sudo - yum remove mssql-tools unixODBC-utf16 unixODBC-utf16-devel
- sudo yum install -y mssql-tools18 unixODBC-devel