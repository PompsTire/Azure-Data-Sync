-- ========================================================================================
-- Create User as DBO template for Azure SQL Database and Azure SQL Data Warehouse Database
-- ========================================================================================
-- For login <login_name, sysname, login_name>, create a user in the database
CREATE USER <user_name, sysname, user_name>
	FOR LOGIN <login_name, sysname, login_name>
	WITH DEFAULT_SCHEMA = <default_schema, sysname, dbo>
GO

create login pbiadmin with password='x32Ach#01q';


CREATE USER pbiadmin FROM LOGIN pbiadmin


GO


-- Add user to the database owner role
EXEC sp_addrolemember N'db_datawriter', 'pbiadmin'

EXEC sp_addrolemember N'db_datareader', 'pbiadmin'

GO

SELECT *
FROM master.sys.sql_logins


SELECT * from master.sys.sql_logins

SELECT * from master.sys.sysusers