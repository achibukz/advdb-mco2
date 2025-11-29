-- Create tester user for all databases with all privileges
CREATE USER IF NOT EXISTS 'tester'@'%' IDENTIFIED BY 'testpass';
GRANT ALL PRIVILEGES ON *.* TO 'tester'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;