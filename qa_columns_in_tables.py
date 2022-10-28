import pandas as pd
import sys

from datetime import datetime, date
from pandas import ExcelWriter

from db import connect_sql
from mail import send_mail


table_name_sec = 'matrix.cmbs.updates_mortgage_loan_detail'
table_name_dl = 'matrix.updates_delinquency_loan_detail'
table_name_ss = 'matrix.updates_specially_serviced_loan_detail'

# microsoft sql connection
connection_sql = connect_sql(
    server_address='10.21.71.211',
    username='dataUser',
    password='Yardi1234'
) 

def main(recipient, cc):
    # Count total rows in table   
    print(f"Extracting data from {table_name_sec}")
    cnt_table_sec = pd.read_sql('select count(*) count_total_rows from matrix.cmbs.updates_mortgage_loan_detail', connection_sql)
    cnt_table_sec = cnt_table_sec['count_total_rows'][0]    
    print('\t{} Total rows into table!'.format(cnt_table_sec))

    cnt_cik_sec = pd.read_sql('select count(distinct cik) count_total_cik from matrix.cmbs.updates_mortgage_loan_detail', connection_sql)
    cnt_cik_sec = cnt_cik_sec['count_total_cik'][0]    
    print('\t{} Total distinct cik!\n'.format(cnt_cik_sec))

    queries_sec = {
        'sec':{
            'query':
            '''with md as (
            select distinct cik, maturity_date, len(maturity_date) my_len, substring(maturity_date, 1, 2) my_sub, 
            case when substring(maturity_date, 1, 2) like '%/%' then cast(substring(maturity_date, 1, 1) as int)
            else cast(substring(maturity_date, 1, 2) as int) 
            end my_ca
            from matrix.cmbs.updates_mortgage_loan_detail
            )
            , mdn as(
           select distinct cik, count(maturity_date) cnt from matrix.cmbs.updates_mortgage_loan_detail 
           where maturity_date is null
           group by cik)
            , os as (
           select distinct cik, operating_stmnt, len(operating_stmnt) my_len,substring(operating_stmnt, 1, 2) my_sub, 
                 case when substring(operating_stmnt, 1, 2) like '%/%' then cast(substring(operating_stmnt, 1, 1) as int)
                     else cast(substring(operating_stmnt, 1, 2) as int) 
                         end my_ca
           from matrix.cmbs.updates_mortgage_loan_detail
            )
            , pd as (
          select distinct cik, prepay_date,len(prepay_date) my_len, substring(prepay_date, 1, 2) my_sub, 
                case when substring(prepay_date, 1, 2) like '%/%' then cast(substring(prepay_date, 1, 1) as int)
                    else cast(substring(prepay_date, 1, 2) as int) 
                        end my_ca
          from matrix.cmbs.updates_mortgage_loan_detail
            )
            , ard as (
          select distinct cik, anticipated_repayment_date, len(anticipated_repayment_date) my_len, substring(anticipated_repayment_date, 1, 2) my_sub, 
                 case when substring(anticipated_repayment_date, 1, 2) like '%/%' then cast(substring(anticipated_repayment_date, 1, 1) as int)
                     else cast(substring(anticipated_repayment_date, 1, 2) as int) 
                         end my_ca
           from matrix.cmbs.updates_mortgage_loan_detail            
            )
            , aprd as (
           select cik, appraisal_reduction_date, len(appraisal_reduction_date) my_len, substring(appraisal_reduction_date, 1, 2) my_sub, 
                 case when substring(appraisal_reduction_date, 1, 2) like '%/%' then cast(substring(appraisal_reduction_date, 1, 1) as int)
                     else cast(substring(appraisal_reduction_date, 1, 2) as int) 
                     end my_ca
           from matrix.cmbs.updates_mortgage_loan_detail           
            )
            , patd as (
           select distinct cik, paid_through_date, len(paid_through_date) my_len, substring(paid_through_date, 1, 2) my_sub, 
           case when substring(paid_through_date, 1, 2) like '%/%' then cast(substring(paid_through_date, 1, 1) as int)
                else cast(substring(paid_through_date, 1, 2) as int) 
                    end my_ca
                from matrix.cmbs.updates_mortgage_loan_detail
                )
  
            select distinct 'maturity_date'error_type, maturity_date error, cik from md where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'maturity_date_null'error_type, cnt eror, cik from mdn where cnt > 3
            union
            select distinct 'operating_stmnt'error_type, operating_stmnt error, cik from os where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'prepay_date'error_type, prepay_date error, cik from pd where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'anticipated_repayment_date'error_type, anticipated_repayment_date error, cik from ard where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'appraisal_reduction_date'error_type, appraisal_reduction_date error, cik from aprd where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select  'paid_through_date'error_type, paid_through_date error, cik from patd where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'current_note_rate'error_type, current_note_rate, cik from matrix.cmbs.updates_mortgage_loan_detail where current_note_rate is null and prepay_date is null and cast(maturity_date as date) > CAST( GETDATE() AS Date)
            union
            select distinct 'current_note_rate_length'error_type,  len(current_note_rate) error, cik from matrix.cmbs.updates_mortgage_loan_detail where len(current_note_rate)>5
            union
            select distinct 'state'error_type, state error, cik from matrix.cmbs.updates_mortgage_loan_detail where state not LIKE '[A-Z][A-Z]' and state not like 'Var%'
            union
            select distinct 'property_type'error_type, property_type error, cik from matrix.cmbs.updates_mortgage_loan_detail where (property_type like 'XX' or property_type not like '[A-Z]%')
            union
            select distinct 'workout_strategy'error_type,  workout_strategy error, cik from matrix.cmbs.updates_mortgage_loan_detail where workout_strategy  not in ('DPO', 'REO', 'TBD') and workout_strategy  not like '[A-Z]%'
            union
            select distinct 'modification_code'error_type, modification_code error, cik from matrix.cmbs.updates_mortgage_loan_detail where modification_code  not like '[A-Z]%'
            union
            select distinct 'defeased'error_type, defeased error, cik from matrix.cmbs.updates_mortgage_loan_detail where defeased not like '[A-Z]%'
            union
            select distinct 'payment_status'error_type, payment_status error, cik from matrix.cmbs.updates_mortgage_loan_detail where payment_status not like '[A-Z]%' and payment_status  not like '[^A-Z]%'
            ''',
            'sheet':'Stats'
        }
    }
    
    # Delinquency table
    print(f"Extracting data from {table_name_dl}")
    cnt_table_dl = pd.read_sql('select count(*) count_total_rows from matrix.cmbs.updates_delinquency_loan_detail', connection_sql)
    cnt_table_dl = cnt_table_dl['count_total_rows'][0]    
    print('\t{} Total rows into table!'.format(cnt_table_dl))

    cnt_cik_dl = pd.read_sql('select count(distinct cik) count_total_cik from matrix.cmbs.updates_delinquency_loan_detail', connection_sql)
    cnt_cik_dl = cnt_cik_dl['count_total_cik'][0]    
    print('\t{} Total distinct cik!\n'.format(cnt_cik_dl))

    queries_dl = {
        'dl':{
            'query':
            '''with patd as (
                select cik, paid_through_date, len(paid_through_date) my_len, substring(paid_through_date, 1, 2) my_sub, 
                 case when substring(paid_through_date, 1, 2) like '%/%' then cast(substring(paid_through_date, 1, 1) as int)
                     else cast(substring(paid_through_date, 1, 2) as int) 
                         end my_ca
                from matrix.cmbs.updates_delinquency_loan_detail
                )
                , std as (
                select cik, paid_through_date, servicing_transfer_date, len(servicing_transfer_date) my_len, substring(servicing_transfer_date, 1, 2) my_sub, 
                 case when substring(servicing_transfer_date, 1, 2) like '%/%' then cast(substring(servicing_transfer_date, 1, 1) as int)
                    else cast(substring(servicing_transfer_date, 1, 2) as int) 
                 end my_ca
                from matrix.cmbs.updates_delinquency_loan_detail
                )
                , fd as (
                select cik, foreclosure_date, len(foreclosure_date) my_len, substring(foreclosure_date, 1, 2) my_sub, 
                case when substring(foreclosure_date, 1, 2) like '%/%' then cast(substring(foreclosure_date, 1, 1) as int)
                else cast(substring(foreclosure_date, 1, 2) as int) 
                end my_ca, max(insert_date)ins_date
               from matrix.cmbs.updates_delinquency_loan_detail
                group by cik, foreclosure_date
                )
                , rd as (
                select cik, reo_date, len(reo_date) my_len, substring(reo_date, 1, 2) my_sub, 
                case when substring(reo_date, 1, 2) like '%/%' then cast(substring(reo_date, 1, 1) as int)
                else cast(substring(reo_date, 1, 2) as int) 
                end my_ca
               from matrix.cmbs.updates_delinquency_loan_detail
                )
                , bd as(
                select cik, bankruptcy_date, len(bankruptcy_date) my_len, substring(bankruptcy_date, 1, 2) my_sub, 
                case when substring(bankruptcy_date, 1, 2) like '%/%' then cast(substring(bankruptcy_date, 1, 1) as int)
                else cast(substring(bankruptcy_date, 1, 2) as int) 
                end my_ca
               from matrix.cmbs.updates_delinquency_loan_detail  
                )
                , ad as(
                select cik, ara_date, len(ara_date) my_len, substring(ara_date, 1, 2) my_sub, 
                case when substring(ara_date, 1, 2) like '%/%' then cast(substring(ara_date, 1, 1) as int)
                else cast(substring(ara_date, 1, 2) as int) 
                end my_ca
               from matrix.cmbs.updates_delinquency_loan_detail
                )
                select distinct 'paid_through_date'error_type, paid_through_date error, cik from patd where my_len > 10 or my_sub like '%/%' or my_ca > 12
                union
                select distinct 'servicing_transfer_date'error_type, servicing_transfer_date error, cik  from std where my_len > 10 or my_sub like '%/%' or my_ca > 12
                union
                select distinct 'foreclosure_date'error_type, foreclosure_date error, cik from fd where my_len > 10 or my_sub like '%/%' or my_ca > 12
                union
                select distinct 'reo_date'error_type, reo_date error, cik from rd where my_len > 10 or my_sub like '%/%' or my_ca > 12            
                union
                select 'bankruptcy_date'error_type, bankruptcy_date error, cik from bd where my_len > 10 or my_sub like '%/%' or my_ca > 12
                union
                select distinct 'ara_date'error_type, ara_date error, cik from ad where my_len > 10 or my_sub like '%/%' or my_ca > 12 
                union
                select 'dscr'error_type, dscr error, cik from matrix.cmbs.updates_delinquency_loan_detail where len(dscr)>5
                union
                select 'ltv'error_type, ltv error, cik from matrix.cmbs.updates_delinquency_loan_detail where len(ltv)>5
                union
                select distinct 'payment_status'error_type, payment_status error, cik from matrix.cmbs.updates_delinquency_loan_detail where payment_status not like '[A-Z]%' and payment_status  not like '[^A-Z]%'
                union
                select distinct 'workout_strategy'error_type, workout_strategy error, cik from matrix.cmbs.updates_delinquency_loan_detail 
                where workout_strategy  not in ('DPO', 'REO', 'TBD') and workout_strategy  not like '[A-Z]%'
            ''',
            'sheet':'Stats'
        }
    }

    # Specially serviced table   
    print(f"Extracting data from {table_name_ss}")
    cnt_table_ss = pd.read_sql('select count(*) count_total_rows from matrix.cmbs.updates_specially_serviced_loan_detail', connection_sql)
    cnt_table_ss = cnt_table_ss['count_total_rows'][0]    
    print('\t{} Total rows into table!'.format(cnt_table_ss))

    cnt_cik_ss = pd.read_sql('select count(distinct cik) count_total_cik from matrix.cmbs.updates_specially_serviced_loan_detail', connection_sql)
    cnt_cik_ss = cnt_cik_ss['count_total_cik'][0]    
    print('\t{} Total distinct cik!\n'.format(cnt_cik_ss))

    queries_ss = {
        'ss':{
            'query':
            '''with patd as (
                select cik, paid_through_date, len(paid_through_date) my_len, substring(paid_through_date, 1, 2) my_sub, 
                 case when substring(paid_through_date, 1, 2) like '%/%' then cast(substring(paid_through_date, 1, 1) as int)
                     else cast(substring(paid_through_date, 1, 2) as int) 
                         end my_ca
                from matrix.cmbs.updates_specially_serviced_loan_detail
                )
            ,std as (
                select cik, paid_through_date, servicing_transfer_date, len(servicing_transfer_date) my_len, substring(servicing_transfer_date, 1, 2) my_sub, 
                case when substring(servicing_transfer_date, 1, 2) like '%/%' then cast(substring(servicing_transfer_date, 1, 1) as int)
                else cast(substring(servicing_transfer_date, 1, 2) as int) 
                end my_ca
                from matrix.cmbs.updates_delinquency_loan_detail
                )
                    , dd as(
                 select cik, dscr_date, len(dscr_date) my_len, substring(dscr_date, 1, 2) my_sub, 
                 case when substring(dscr_date, 1, 2) like '%/%' then cast(substring(dscr_date, 1, 1) as int)
                 else cast(substring(dscr_date, 1, 2) as int) 
                 end my_ca
                from matrix.cmbs.updates_specially_serviced_loan_detail
                )
            , md as (
               select cik, maturity_date, len(maturity_date) my_len, 
                substring(maturity_date, 1, 2) my_sub, 
                case when substring(maturity_date, 1, 2) like '%/%' then cast(substring(maturity_date, 1, 1) as int)
                else cast(substring(maturity_date, 1, 2) as int) 
                end my_ca
                from matrix.cmbs.updates_specially_serviced_loan_detail
                )
            , ad as (
                select cik, appraisal_date, len(appraisal_date) my_len, 
                substring(appraisal_date, 1, 2) my_sub, 
                case when substring(appraisal_date, 1, 2) like '%/%' then cast(substring(appraisal_date, 1, 1) as int)
                else cast(substring(appraisal_date, 1, 2) as int) 
                end my_ca
                from matrix.cmbs.updates_specially_serviced_loan_detail 
                )
            select distinct 'paid_through_date'error_type, paid_through_date error, cik from patd where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct  'servicing_transfer_date'error_type, servicing_transfer_date error, cik from std where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'dscr_date'error_type, dscr_date error, cik from dd where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'maturity_date'error_type, maturity_date error, cik from md where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct 'appraisal_date'error_type, appraisal_date error, cik from ad where my_len > 10 or my_sub like '%/%' or my_ca > 12
            union
            select distinct  'dscr'error_type, dscr error, cik from matrix.cmbs.updates_specially_serviced_loan_detail where len(dscr)>7 and dscr not like '-%'
            union
            select distinct  'workout_strategy'error_type, workout_strategy error, cik from matrix.cmbs.updates_specially_serviced_loan_detail where workout_strategy  not in ('DPO', 'REO', 'TBD') and workout_strategy  not like '[A-Z]%'
            union
            select distinct 'state'error_type, state erorr, cik from matrix.cmbs.updates_specially_serviced_loan_detail where state not LIKE '[A-Z][A-Z]' and state not like 'Var%'
            union
            select distinct 'property_type'error_type, property_type error, cik from matrix.cmbs.updates_specially_serviced_loan_detail where (property_type like 'XX' or property_type like '[A-Z][A-Z]') group by cik, property_type
            union
            select distinct 'interest_rate'error_type, interest_rate error, cik from matrix.cmbs.updates_specially_serviced_loan_detail where len(interest_rate)>5
            union
            select distinct 'interest_rate'error_type, loan_status error, cik from matrix.cmbs.updates_specially_serviced_loan_detail where loan_status not like '[A-Z]%' and loan_status  not like '[^A-Z]%'
            ''',
            'sheet':'Stats'
        }
    }

    # Create xlsx files
    mortgage_files = save_xlsx(queries_sec, 'Mortgage')
    delinquency_files = save_xlsx(queries_dl, 'Delinquency')
    specially_files = save_xlsx(queries_ss, 'Specially_Serviced')

    # Send email
    send_results(
        mortgage_files + delinquency_files + specially_files, recipient, cc,
        cnt_table_sec, cnt_cik_sec, cnt_table_dl, cnt_cik_dl,
        cnt_table_ss, cnt_cik_ss)
   

def send_results(
        files_to_send, recipient, cc, 
        cnt_table_sec, cnt_cik_sec,
        cnt_table_dl, cnt_cik_dl,
        cnt_table_ss, cnt_cik_ss):
    print("\nPreparing the email...")
    current_date = date.today().isoformat() 

    if len(files_to_send) == 0:
        send_mail(
            sender='SMO-MatrixCMBS@yardi.com',
            recipient=recipient,
            cc=cc,
            subject=f'Stats for QA Data SEC Loans Updates - {current_date}',
            message='Hi,\n\n There are no errors currently on updates loans tables...',
            filepath=[]
            )  
    else:
        send_mail(
            sender='SMO-MatrixCMBS@yardi.com',
            recipient=recipient,
            cc=cc,
            subject=f'Stats for QA Data SEC Loans Updates - {current_date}',
            message='Hi,\n\n'
            'This is a test report following the verification of the data columns in sec, delinquency and specially serviced loans updates table.\n\n'
            'For this qa session, total rows in table and total distinct cik are:\n'
            '\t matrix.cmbs.updates_mortgage_loan_detail = ' + str(cnt_table_sec) + ' and ' + str(cnt_cik_sec) + '\n'
            '\t matrix.updates_delinquency_loan_detail = ' + str(cnt_table_dl) + ' and ' + str(cnt_cik_dl) + '\n'
            '\t matrix.updates_specially_serviced_loan_detail = ' + str(cnt_table_ss) + ' and ' + str(cnt_cik_ss) + '\n\n'
            'The columns we are interested in for QA are: maturity date, neg amort, current note rate, state, property type, workout strategy, modification code, defeased, payment status, operating stmnt, prepay date, anticipated repayment date, appraisal reduction date, paid through date, servicing transfer date, interest rate, dscr date, appraisal date, loan status, dscr, ltv, foreclosure date, reo date, bankruptcy date, ara date.\n\n'
            'If you encounter any problem with the file, please let me know.\n\n'
            'Regards,\n' 
            'Matrix Data Team', 
            filepath=files_to_send
        )


def save_xlsx(queries, source):
    # Export data in cs
    current_date = date.today().isoformat() 
    files_to_send = []
    xlsx_name = f"QA_Sec_{source}_Loans_Updates_" + current_date + '.xlsx'

    for values in queries.values():
        data = pd.read_sql(values['query'], con=connection_sql)
        if not data.empty:
            print('Creating file {0}'.format(xlsx_name))
            writer = ExcelWriter(xlsx_name)
            data.to_excel(writer, '{0}_errors'.format(values['sheet']), index=False, float_format='%g')
            writer.save()
            files_to_send.append(xlsx_name)
        else:
            print(f'Queries {source} returned 0 results, nothing to write...')
    
    return files_to_send


if __name__ == "__main__":
    startTime = datetime.now()

    if len(sys.argv) != 3:
        print("Please run with recipient and cc parameters")
        exit()

    print(f'Starting script at {startTime}\n')

    recipient = sys.argv[1]
    cc = sys.argv[2]
    main(recipient, cc)     
    print(datetime.now() - startTime)
