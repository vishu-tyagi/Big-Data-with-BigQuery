select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trip_distance
from `test-project-kkj`.`staging`.`stg_green_tripdata`
where trip_distance is null



      
    ) dbt_internal_test