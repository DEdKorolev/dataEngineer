/*0 Проверка meta_increment на наличие записей
При отсутвии установка записей по умолчанию*/
insert into meta_increment
select to_timestamp('01.01.1800 00:00:00', 'dd/mm/yyyy HH24:MI:SS')
from dual
where (select count(*) from meta_increment) = 0
//


/*1 Чистим стейджинг и загружаем инкремент*/
delete from stg_increment
//

insert into stg_increment
select src.*  from src_increment src
where src."date" >
                    (select max(max_date) from meta_increment
                    )
//


/*2 Укладываем insert, update-записи */

--2.1 dim_terminals

merge into dim_terminals dt
using (select distinct  s."date",
						s.terminal,
                        s.terminal_type,
                        s.city,
                        s.address
        from stg_increment s left join dim_terminals d on d.terminal_id=s.terminal
        where s."date" in
						(select distinct max("date")
						over (partition by terminal) 
						from stg_increment)
				and
						(coalesce(d.terminal_type, '0')<>s.terminal_type
						or coalesce(d.terminal_city, '0')<>s.city
						or coalesce(d.terminal_address, '0')<>s.address)
			) stg
on (dt.terminal_id=stg.terminal)
when matched then update set 	dt.terminal_type=stg.terminal_type,
								dt.terminal_city=stg.city,
								dt.terminal_address=stg.address,
                                dt.update_dt =  cast(stg."date" as date)
when not matched then insert (	dt.terminal_id,
								dt.terminal_type,
								dt.terminal_city,
								dt.terminal_address,
                                dt.create_dt)
                    values (	stg.terminal,
								stg.terminal_type,
								stg.city,
								stg.address,
                                cast(stg."date" as date)
                                )
//
								

--2.2 dim_clients

merge into dim_clients dcl
using (select distinct  s."date",
                        s.client,
                        s.last_name,
                        s.first_name,
                        s.patronymic,
						s.date_of_birth,
						s.passport,
						s.passport_valid_to,
						s.phone
    from stg_increment s left join dim_clients d on d.client_id=s.client
    where s."date" in
					(select distinct max("date")
					over (partition by client) 
					from stg_increment)
            and
					(coalesce(d.last_name, '0')<>s.last_name
					or coalesce(d.first_name, '0')<>s.first_name
					or coalesce(d.patronymic, '0')<>s.patronymic
					or coalesce(d.date_of_birth, to_date('01.01.1800', 'dd/mm/yyyy'))<>s.date_of_birth
					or coalesce(d.passport_num, '0')<>s.passport
					or coalesce(d.passport_valid_to, to_date('01.01.1800', 'dd/mm/yyyy'))<>s.passport_valid_to
					or coalesce(d.phone, '0')<>s.phone)
            ) stg
on (dcl.client_id=stg.client)
when matched then update set 	
                                dcl.last_name=stg.last_name,
                                dcl.first_name=stg.first_name,
                                dcl.patronymic=stg.patronymic,
                                dcl.date_of_birth=stg.date_of_birth,
                                dcl.passport_num=stg.passport,
                                dcl.passport_valid_to=stg.passport_valid_to,
                                dcl.phone=stg.phone,
                                dcl.update_dt =  cast(stg."date" as date)
when not matched then insert (	dcl.client_id,
								dcl.last_name,
								dcl.first_name,
								dcl.patronymic,
								dcl.date_of_birth,
								dcl.passport_num,
								dcl.passport_valid_to,
								dcl.phone,
                                dcl.create_dt)
                    values (	stg.client,
								stg.last_name,
								stg.first_name,
								stg.patronymic,
								stg.date_of_birth,
								stg.passport,
								stg.passport_valid_to,
								stg.phone,
                                cast(stg."date" as date))
//
					

--2.3 dim_accounts
								
merge into dim_accounts da
using (select distinct  s."date",
						s.account,
                        s.account_valid_to,
                        s.client
        from stg_increment s left join dim_accounts d on d.account_num=s.account
        where s."date" in
						(select distinct max("date")
						over (partition by account) 
						from stg_increment)
				and
						(coalesce(d.valid_to, to_date('01.01.1800', 'dd/mm/yyyy'))<>s.account_valid_to
						or coalesce(d.client, '0')<>s.client)		
			) stg
on (da.account_num=stg.account)
when matched then update set 	da.valid_to=stg.account_valid_to,
								da.client=stg.client,
                                da.update_dt =  cast(stg."date" as date)
when not matched then insert (	da.account_num,
								da.valid_to,
								da.client,
                                da.create_dt)
                    values (	stg.account,
								stg.account_valid_to,
								stg.client,
                                cast(stg."date" as date)
                                )
//
									

--2.4 dim_cards
                       
merge into dim_cards dc
using (select distinct  s."date",
						s.card,
                        s.account
        from stg_increment s left join dim_cards d on d.card_num=s.card
        where s."date" in
						(select distinct max("date")
						over (partition by card) 
						from stg_increment)
				and
						coalesce(d.account_num, '0')<>s.account
			) stg
on (dc.card_num=stg.card)
when matched then update set 	dc.account_num=stg.account,
                                dc.update_dt =  cast(stg."date" as date)
when not matched then insert (	dc.card_num,
								dc.account_num,
                                dc.create_dt)
                    values (	stg.card,
								stg.account,
                                cast(stg."date" as date)
                                )
//
								

--2.5 fact_transactions							

merge into fact_transactions ft
using stg_increment stg
on (ft.trans_id=stg.trans_id)
when matched then update set 	
								ft.trans_date=stg."date",
								ft.card_num=stg.card,
								ft.oper_type=stg.oper_type,
								ft.amt=stg.amount,
								ft.oper_result=stg.oper_result,
								ft.terminal=stg.terminal
when not matched then insert (	ft.trans_id,
								ft.trans_date,
								ft.card_num,
								ft.oper_type,
								ft.amt,
								ft.oper_result,
								ft.terminal)
                    values (	stg.trans_id,
								stg."date",
								stg.card,
								stg.oper_type,
								stg.amount,
								stg.oper_result,
								stg.terminal)
//
								
											
/*3 Обновление мета-данных*/
update meta_increment set max_date = (
        select coalesce((
            select max("date") 
            from stg_increment), max_date) 
        from meta_increment)
//
		

/*4 Очитска источника*/
delete src_increment