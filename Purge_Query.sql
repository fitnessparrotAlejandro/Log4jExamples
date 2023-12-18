CREATE TABLE [dbo].[USER_query_result_age](
    [table_name] [varchar](50) NOT NULL,
    [unique_id] [int] NOT NULL,
    [row_count] [int] NOT NULL,
    [created_time] [datetime] NOT NULL DEFAULT GETDATE())
)
GO

GRANT SELECT ON [dbo].[USER_query_result_age] TO [olf_readonly] AS [dbo]
GRANT DELETE ON [dbo].[USER_query_result_age] TO [olf_user] AS [dbo]
GRANT INSERT ON [dbo].[USER_query_result_age] TO [olf_user] AS [dbo]
GRANT SELECT ON [dbo].[USER_query_result_age] TO [olf_user] AS [dbo]
GRANT UPDATE ON [dbo].[USER_query_result_age] TO [olf_user] AS [dbo]
GO

CREATE UNIQUE CLUSTERED INDEX [idx_user_query_result] ON [dbo].[USER_query_result_age]
  ([table_name] ASC,
  [unique_id] ASC)
GO



CREATE PROCEDURE [dbo].[USER_PURGE_Query_Results]
AS

DECLARE @resultRetentionHours INT;

DECLARE @strResultRetentionHours VARCHAR(3);
DECLARE @narme VARCHAR(50);
DECLARE @QRY VARCHAR(4000);

-- Default to 6 hours --
SELECT @resultRetentionHours = int_value FROM USER_const_repository WHERE context = 'UTIL' and sub_context = 'PURGE' AND name = 'Query_Result_Hours_Retention';
IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO USER_const_repository VALUES ('UTIL', 'PURGE', 'Query_Result_Hours_Retention', 0, 0, 6, NULL, 0);
    SET @resultRetentionHours = 6;
END

SET @strResultRetentionHours = CONVERT(VARCHAR(3), @resultRetentionHours);

-- Get list of Tables to Purge --
DECLARE thl_cursor CURSOR FOR
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME LIKE 'QUERY_RESULT%' AND TABLE_NAME NOT IN ('USER_query_result', 'query_result_alert');

OPEN tbl_cursor
FETCH NEXT FROM tbl cursor INTO @name

WHILE @@FETCH_STATUS = 0
BEGIN

	SET @QRY ='';
	  /** Insert into Age Table **/
	SET @QRY = @QRY + 'insert into USER_Query Result Age ';
	SET @QRY = @QRY + 'select table_name, unique_id, cnt, GETDATE() FROM ';
	SET @QRY = @QRY + '(select " + @name + " as table name, unique_id, count(1) as cnt from ' + @name + ' with (nolock) group by unique_id ';
	SET @QRY = @QRY + 'except';
	SET @QRY = @QRY + 'select table_name, unique_id, row_count from USER_Query Result Age) T; ';
	EXECUTE (@QRY) ;

	/** Delete from query table where older than threshold **/
	SET @QRY = '';
	SET @QRY = @QRY + 'DELETE ' + @name + ';';
	SET @QRY = @QRY + 'FROM ' + @name + ' g INNER JOIN USER_QueryResultAge a ON (g.unique_id = a.unique_id AND a.table_name = ' + @name + ');';
	SET @QRY = @QRY + 'WHERE DATEDIFF(hour, created_time, GETDATE()) >= ' + @strResultRetentionHours + ';';

	EXECUTE (@QRY);
	/** Delete from USER_Query_Result_Age where we have just purged **/
	SET @QRY = 'delete from USER_Query_Result_Age where table_name ="' + @name + '" and DATEDIFF(hour, created_time, GETDATE()) >= ' + @strResultRetentionHours + ';'

	EXECUTE (@QRY);

FETCH NEXT FROM tbl_cursor INTO @name
END

CLOSE tbl cursor
DEALLOCATE tbl_cursor

GO

GRANT EXECUTE ON USER_PURGE_Query_Results TO [olf user];