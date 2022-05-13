MERGE INTO report rep
USING (
--��������������� ������� ��� ������
    WITH for_report AS (
        SELECT ft.trans_date fraud_dt,
                LEAD (ft.trans_date) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_date,
                LEAD (ft.trans_date, 2) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_date_2,
                LEAD (ft.trans_date, 3) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_date_3,
                ft.amt amt,
                LEAD (ft.amt) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_amt,
                LEAD (ft.amt, 2) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_amt_2,
                LEAD (ft.amt, 3) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_amt_3,
                ft.oper_result oper_result,
                LEAD (ft.oper_result) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_oper_result,
                LEAD (ft.oper_result, 2) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_oper_result_2,
                LEAD (ft.oper_result, 3) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_oper_result_3,
                ft.oper_type oper_type,
                LEAD (ft.oper_type) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_oper_type,
                LEAD (ft.oper_type, 2) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_oper_type_2,
                LEAD (ft.oper_type, 3) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_oper_type_3,
                dcl.passport_num passport,
                dcl.last_name || ' ' || first_name || ' ' || patronymic fio,
                dcl.phone,
                dt.terminal_city city,
                LEAD (dt.terminal_city) OVER (PARTITION BY dcl.client_id ORDER BY ft.trans_date) lead_city,
                systimestamp report_dt,
                da.valid_to acc_valid_to,
                dcl.passport_valid_to passport_valid_to,
                CAST(ft.trans_date AS date) trans_date
                
        FROM dim_clients dcl
            INNER JOIN dim_accounts da ON dcl.client_id = da.client
            INNER JOIN dim_cards dc ON da.account_num = dc.account_num
            INNER JOIN fact_transactions ft ON dc.card_num = ft.card_num
            INNER JOIN dim_terminals dt ON ft.terminal = dt.terminal_id
        
		-- ����� ��������� ��� ����� �������� ������ 25/24 ����
        WHERE ft.trans_date > (
								SELECT max(max_date)
								FROM meta_increment
								) - 25/24 
        )
    
	
	--������ ��� ��������� ���������� �������� ��� ������������ ��������.
	SELECT 	fraud_dt,
			passport,
			fio,
			phone,
			'���������� �������� ��� ������������ ��������' fraud_type,
			report_dt
	FROM for_report
	WHERE passport_valid_to < trans_date

	UNION ALL

	--������ ��� ��������� ���������� �������� ��� ������������� ��������.
	SELECT fraud_dt,
			passport,
			fio,
			phone,
			'���������� �������� ��� ������������� ��������' fraud_type,
			report_dt
	FROM for_report
	WHERE acc_valid_to < trans_date    
	   
	UNION ALL

	--������ ��� ��������� ���������� �������� � ������ ������� � ������� 1 ����.    
	SELECT  lead_date fraud_dt,
			passport,
			fio,
			phone,
			'���������� �������� � ������ ������� � ������� 1 ����' fraud_type,
			report_dt
	FROM for_report       
	WHERE city <> lead_city
			AND EXTRACT(DAY FROM (lead_date - fraud_dt))*24 + EXTRACT(HOUR FROM (lead_date - fraud_dt)) < 1
			AND amt = lead_amt
            AND lead_oper_type = oper_type

	UNION ALL

	--������ ��� ��������� ������� ������� ����. 
	--����� �������� ����� 3� �������� �� 20 �����, ������� �������� �� �� ���������� �������:
	--������ ����������� ������ ����������, ��� ���� ��������� ��� ����� ���������. ��������� �������� (��������) � ����� ������� ��������� �������������.
	SELECT  lead_date_3 fraud_dt,
			passport,
			fio,
			phone,
			'������� ������� ����' fraud_type,
			report_dt
	FROM for_report
	WHERE EXTRACT(DAY FROM (lead_date_3 - fraud_dt))*60*24 + EXTRACT(HOUR FROM (lead_date_3 - fraud_dt))*60 + EXTRACT(MINUTE FROM (lead_date_3 - fraud_dt))<=20
			AND lead_amt_3 - lead_amt_2 < 0
			AND lead_amt_2 - lead_amt < 0
            AND lead_amt - amt < 0
			AND lead_oper_result_3 = '�������'
			AND lead_oper_result_2 = '�����'
			AND lead_oper_result = '�����'
            AND oper_result = '�����'
            AND lead_oper_type_3 in ('������', '������')
            AND lead_oper_type_2 in ('������', '������')
            AND lead_oper_type in ('������', '������')
            AND oper_type in ('������', '������')
	) fr

--���������� �� �����	
ON (rep.fraud_dt = fr.fraud_dt 
    AND rep.passport = fr.passport
    AND rep.fraud_type = fr.fraud_type)

--���� ��� ����������, �� �������� ������
WHEN NOT matched 
THEN INSERT (  
			rep.fraud_dt,
			rep.passport,
			rep.fio,
			rep.phone,
			rep.fraud_type,
			rep.report_dt
			)
VALUES (
			fr.fraud_dt,
			fr.passport,
			fr.fio,
			fr.phone,
			fr.fraud_type,
			fr.report_dt
		)