select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pickup_locationid
from `test-project-kkj`.`staging`.`stg_green_tripdata`
where pickup_locationid is null



      
    ) dbt_internal_test