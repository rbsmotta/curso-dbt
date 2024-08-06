with calc_employees as (
    select 
        *,
        date_diff(current_date, birth_date, year) as age,
        date_diff(current_date, hire_date, year) as lengthofservice,
        -- concat(first_name, ' ', last_name),
        first_name || ' ' || last_name as name
    from {{ source('sources', 'employees') }}
)

select * from calc_employees