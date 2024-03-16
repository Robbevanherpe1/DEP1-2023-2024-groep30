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

### Install the SQL Server command-line tools
- curl https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo (Red Hat 9)
- sudo - yum remove mssql-tools unixODBC-utf16 unixODBC-utf16-devel
- sudo yum install -y mssql-tools18 unixODBC-develyze
- sudo yum check-update sudo yum update mssql-tools18
- echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bash_profile
- echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc source ~/.bashrc

### Connect locally
- sqlcmd -S localhost -U sa -P 'VMdepgroup30' -Ce

### Create and query data
- CREATE DATABASE TestDB;
- SELECT Name from sys.databases;
- GO

- USE TestDB;
- CREATE TABLE dbo.Inventory ( id INT, name NVARCHAR(50), quantity INT, PRIMARY KEY (id) );
- INSERT INTO dbo.Inventory VALUES (1, 'banana', 150);
- INSERT INTO dbo.Inventory VALUES (2, 'orange', 154);
- GO

- SELECT * FROM dbo.Inventory WHERE quantity > 152;
- GO

- QUIT

## Connecteren met SQL Server

- ssh -L 1500:localhost:1433 10.11.11.30
