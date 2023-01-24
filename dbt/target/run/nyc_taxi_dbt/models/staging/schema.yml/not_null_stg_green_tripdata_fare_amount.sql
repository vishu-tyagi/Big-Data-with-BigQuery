select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fare_amount
from `test-project-kkj`.`staging`.`stg_green_tripdata`
where fare_amount is null



      
    ) dbt_internal_test