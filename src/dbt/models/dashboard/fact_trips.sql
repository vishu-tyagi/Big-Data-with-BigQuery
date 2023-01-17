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

select
    trips.vendorid
    , trips.service_type
    , trips.ratecodeid
    , trips.pickup_locationid
    , pickup.borough as pickup_borough
    , pickup.zone as pickup_zone
    , trips.dropoff_locationid
    , dropoff.borough as dropoff_borough
    , dropoff.zone as dropoff_zone
    , trips.pickup_datetime
    , trips.dropoff_datetime
    , trips.Store_and_fwd_flag
    , trips.passenger_count
    , trips.trip_distance
    , trips.fare_amount
    , trips.extra_amount
    , trips.mta_tax
    , trips.tip_amount
    , trips.toll_fee
    , trips.improvement_surcharge
    , trips.total_amount
    , trips.payment_type
from
    trips_unioned trips
    inner join {{ ref('dim_taxi_zone_lookup') }} pickup
    on trips.pickup_locationid = pickup.locationid
    inner join {{ ref('dim_taxi_zone_lookup') }} dropoff
    on trips.dropoff_locationid = dropoff.locationid

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
