select * from src_increment;
select * from meta_increment;
select * from stg_increment;

select * from dim_accounts;
select * from dim_cards;
select * from dim_clients;
select * from dim_terminals;
select * from fact_transactions;

select * from dim_accounts_history;
select * from dim_cards_history;
select * from dim_clients_history;
select * from dim_terminals_history;

select * from report;
select * from report where fraud_type = '���������� �������� ��� ������������� ��������';
select * from report where fraud_type = '���������� �������� ��� ������������ ��������';
select * from report where fraud_type = '���������� �������� � ������ ������� � ������� 1 ����';
select * from report where fraud_type = '������� ������� ����';
