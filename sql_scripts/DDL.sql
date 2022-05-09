drop table src_increment
//
drop table stg_increment
//
drop table meta_increment
//
drop table dim_accounts_history
//
drop table dim_cards_history
//
drop table dim_clients_history
//
drop table dim_terminals_history
//
drop table fact_transactions
//
drop table dim_terminals
//
drop table dim_cards
//
drop table dim_accounts
//
drop table dim_clients
//
drop table report
//

-- SOURCE
create table src_increment (
                               trans_id number not null,
                               "date" date not null,
                               card varchar2(255) not null,
                               account varchar2(255) not null,
                               account_valid_to date not null,
                               client varchar2(255) not null,
                               last_name varchar2(255) not null,
                               first_name varchar2(255) not null,
                               patronymic varchar2(255),
                               date_of_birth date not null,
                               passport number not null,
                               passport_valid_to date not null,
                               phone varchar2(255) not null,
                               oper_type varchar2(255),
                               amount number not null,
                               oper_result varchar2(255) not null,
                               terminal varchar2(255) not null,
                               terminal_type varchar2(255) not null,
                               city varchar2(255) not null,
                               address varchar2(255) not null,
                               primary key (trans_id)
)
//


-- CLIENTS
create table dim_clients (
                             client_id varchar2(255) not null,
                             last_name varchar2(255),
                             first_name varchar2(255),
                             patronymic varchar2(255),
                             date_of_birth date,
                             passport_num varchar2(255),
                             passport_valid_to date,
                             phone varchar2(255),
                             create_dt date,
                             update_dt date,
                             primary key (client_id)
)
//


-- ACCOUNTS
create table dim_accounts (
                              account_num varchar2(255) not null,
                              valid_to date,
                              client varchar2(255),
                              create_dt date,
                              update_dt date,
                              primary key (account_num)
)
//

-- LINK BETWEEN CLIENTS AND ACCOUNTS
alter table dim_accounts
    add constraint dim_accounts_fk_dim_clients
        foreign key (client) references dim_clients
//		

		
-- CARDS
create table dim_cards (
                           card_num varchar2(255) not null,
                           account_num varchar2(255),
                           create_dt date,
                           update_dt date,
                           primary key (card_num)
)
//

-- LINK BETWEEN CARDS AND ACCOUNTS
alter table dim_cards
    add constraint dim_cards_fk_dim_accounts
        foreign key (account_num) references dim_accounts
//

-- TERMINALS
create table dim_terminals (
                               terminal_id varchar2(255) not null,
                               terminal_type varchar2(255),
                               terminal_city varchar2(255),
                               terminal_address varchar2(255),
                               create_dt date,
                               update_dt date,
                               primary key (terminal_id)
)
//

-- CARDS IN TERMINAL
create table fact_transactions (
                                   trans_id varchar2(255),
                                   trans_date timestamp,
                                   card_num varchar2(255) not null,
                                   oper_type varchar2(255),
                                   amt number,
                                   oper_result varchar2(255),
                                   terminal varchar2(255)

)
//

alter table fact_transactions
    add constraint fact_transactions_fk_dim_cards
        foreign key (card_num) references dim_cards
//

alter table fact_transactions
    add constraint fact_transactions_fk_terminals
        foreign key (terminal) references dim_terminals
//


--HISTORY CLIENTS
create table dim_clients_history (
                             client_id varchar2(255) not null,
                             last_name varchar2(255),
                             first_name varchar2(255),
                             patronymic varchar2(255),
                             date_of_birth date,
                             passport_num varchar2(255),
                             passport_valid_to date,
                             phone varchar2(255),
                             start_date date,
                             end_date date
)
//

alter table dim_clients_history
    add constraint dcl_history_fk_dim_clients
        foreign key (client_id) references dim_clients
//


--HISTORY ACCOUNTS
create table dim_accounts_history (
                              account_num varchar2(255) not null,
                              valid_to date,
                              client varchar2(255),
                              start_date date,
                              end_date date
)
//

alter table dim_accounts_history
    add constraint da_history_fk_dim_accounts
        foreign key (account_num) references dim_accounts
//


--HISTORY CARDS
create table dim_cards_history (
                           card_num varchar2(255) not null,
                           account_num varchar2(255),
                           start_date date,
                           end_date date
)
//

alter table dim_cards_history
    add constraint dc_history_fk_dim_cards
        foreign key (card_num) references dim_cards
//


--HISTORY TERMINALS
create table dim_terminals_history (
                               terminal_id varchar2(255) not null,
                               terminal_type varchar2(255),
                               terminal_city varchar2(255),
                               terminal_address varchar2(255),
                               start_date date,
                               end_date date
)
//

alter table dim_terminals_history
    add constraint dt_history_fk_dim_terminals
        foreign key (terminal_id) references dim_terminals
//


--STAGING
create table stg_increment as select * from src_increment where 1=0
//


--METADATA
create table meta_increment
(max_date date
)
//
		

--REPORT
create table report (
                       fraud_dt timestamp not null,
                       passport number not null,
                       fio varchar2(255) not null,
                       phone varchar2(255) not null,
                       fraud_type varchar2(255) not null,
                       report_dt timestamp not null
)
//


--TRIGGER FOR TABLE_HISTORY
----FOR dim_terminals_history
create or replace trigger dim_terminals_update_trg 
    after update on dim_terminals for each row
begin
    insert into dim_terminals_history
    values(   :old.terminal_id
            , :old.terminal_type
            , :old.terminal_city
            , :old.terminal_address
            , coalesce(:old.update_dt, :old.create_dt)
            , :new.update_dt
            );
end;
//

----FOR dim_clients_history
create or replace trigger dim_clients_update_trg 
    after update on dim_clients for each row
begin
    insert into dim_clients_history
    values(   :old.client_id
            , :old.last_name
            , :old.first_name
            , :old.patronymic
            , :old.date_of_birth
            , :old.passport_num
            , :old.passport_valid_to
            , :old.phone
            , coalesce(:old.update_dt, :old.create_dt)
            , :new.update_dt
            );
end;
//

----FOR dim_accounts_history
create or replace trigger dim_accounts_update_trg 
    after update on dim_accounts for each row
begin
    insert into dim_accounts_history
    values(   :old.account_num
            , :old.valid_to
			, :old.client
            , coalesce(:old.update_dt, :old.create_dt)
            , :new.update_dt
            );
end;
//

----FOR dim_cards_history
create or replace trigger dim_cards_update_trg 
    after update on dim_cards for each row
begin
    insert into dim_cards_history
    values(   :old.card_num
            , :old.account_num
            , coalesce(:old.update_dt, :old.create_dt)
            , :new.update_dt
            );
end;
//
