select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dropoff_datetime
from `test-project-kkj`.`staging`.`stg_green_tripdata`
where dropoff_datetime is null



      
    ) dbt_internal_test