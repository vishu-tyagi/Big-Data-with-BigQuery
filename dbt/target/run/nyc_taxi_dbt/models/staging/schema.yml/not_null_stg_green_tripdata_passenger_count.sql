select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select passenger_count
from `test-project-kkj`.`staging`.`stg_green_tripdata`
where passenger_count is null



      
    ) dbt_internal_test