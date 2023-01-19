{{ config(materialized='table')  }}
{{ config(schema='dashboard')  }}


with green_trips as (
    select
        *
        , 'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
)
, yellow_trips as (
    select
        *
        , 'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
)
, trips_unioned as (
    select *
    from green_trips
    union all
    select *
    from yellow_trips
)

-- Reduce data size by grouping for Tableau visualization
select
    trips.vendorid
    , trips.service_type
    , pickup.borough as pickup_borough
    , pickup.zone as pickup_zone
    , dropoff.borough as dropoff_borough
    , timestamp_trunc(pickup_datetime, hour) pickup_datetime
    , sum(trips.trip_distance) trip_distance
    , sum(trips.fare_amount) fare_amount
    , count(*) number_of_rides
from
    trips_unioned trips
    inner join {{ ref('dim_taxi_zone_lookup') }} pickup
    on trips.pickup_locationid = pickup.locationid
    inner join {{ ref('dim_taxi_zone_lookup') }} dropoff
    on trips.dropoff_locationid = dropoff.locationid
group by 1, 2, 3, 4, 5, 6

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
