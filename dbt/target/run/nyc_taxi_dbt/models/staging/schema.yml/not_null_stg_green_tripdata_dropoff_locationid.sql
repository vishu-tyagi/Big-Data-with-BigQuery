select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dropoff_locationid
from `test-project-kkj`.`staging`.`stg_green_tripdata`
where dropoff_locationid is null



      
    ) dbt_internal_test