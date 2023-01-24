
    
    

with all_values as (

    select
        vendorid as value_field,
        count(*) as n_records

    from `test-project-kkj`.`staging`.`stg_green_tripdata`
    group by vendorid

)

select *
from all_values
where value_field not in (
    1,2
)


