

  create or replace view `test-project-kkj`.`staging`.`dim_taxi_zone_lookup`
  OPTIONS()
  as 



select
    locationid
    , borough
    , zone
    , replace(service_zone, 'Boro', 'Green') as service_zone
from `test-project-kkj`.`staging`.`taxi_zone_lookup`
where
    borough != 'Unknown'
    and service_zone != 'N/A'
    and borough != 'EWR';

