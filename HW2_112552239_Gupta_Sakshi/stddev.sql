CREATE PROCEDURE stddev(OUT dev DECIMAL(15,2))
	LANGUAGE SQL
SP:	BEGIN
		DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
		DECLARE sum DECIMAL(15,2);
		DECLARE count DECIMAL(15,2);
		DECLARE sq_sum DECIMAL(15,2);
		DECLARE sal DECIMAL(15,2);
		DECLARE x_mean DECIMAL(15,2);
		DECLARE x2_mean DECIMAL(15,2);
		DECLARE var DECIMAL(15,2);
		DECLARE std_d DECIMAL(15,2);
		DECLARE c CURSOR FOR SELECT SALARY FROM EMPLOYEE;
		SET sum=0;
		SET count=0;
		SET sq_sum=0;
		OPEN c;
		FETCH FROM c INTO sal;
		WHILE(SQLSTATE='00000') DO
			SET sum=sum+sal;
			SET sq_sum=sq_sum+(sal*sal);
			SET count=count+1;
			FETCH FROM c INTO sal;
		END WHILE;
		CLOSE c;
		SET x_mean=sum/count;
		SET x2_mean=sq_sum/count;
		SET var=x2_mean-(x_mean*x_mean);
		SET std_d=SQRT(var);
		SET dev=std_d;
	END SP
@

