


with type_casted as (
    select
        -- identifiers
        cast(vendorid as integer) vendorid
        , cast(ratecodeid as integer) ratecodeid
        , cast(pulocationid as integer) pickup_locationid
        , cast(dolocationid as integer) dropoff_locationid

        -- timestamps
        , cast(lpep_pickup_datetime as timestamp) pickup_datetime
        , cast(lpep_dropoff_datetime as timestamp) dropoff_datetime

        -- trip info
        , case
            when store_and_fwd_flag = 'Y' then true
            when store_and_fwd_flag = 'N' then false
            else null
        end store_and_fwd_flag
        , cast(passenger_count as integer) passenger_count
        , cast(trip_distance as numeric) trip_distance

        -- payment info
        , cast(fare_amount as numeric) fare_amount
        , cast(extra as numeric) extra_amount
        , cast(mta_tax as numeric) mta_tax
        , cast(tip_amount as numeric) tip_amount
        , cast(tolls_amount as numeric) toll_fee
        , cast(improvement_surcharge as numeric) improvement_surcharge
        , cast(total_amount as numeric) total_amount
        , cast(payment_type as integer) payment_type
    from `test-project-kkj`.`staging`.`green_tripdata`
)

, cleaned as (
    select *
    from type_casted
    where
        -- identifiers
        vendorid in (1, 2)
        and ratecodeid in (1, 2, 3, 4, 5, 6, null)
        and pickup_locationid is not null
        and dropoff_locationid is not null

        -- timestamps
        and extract(year from pickup_datetime) in (2019, 2020, 2021)
        and extract(year from dropoff_datetime) in (2019, 2020, 2021)

        -- payment info
        and fare_amount > 0
        and (extra_amount is null or extra_amount >= 0)
        and (mta_tax is null or mta_tax >= 0)
        and (tip_amount is null or tip_amount >= 0)
        and (toll_fee is null or toll_fee >= 0)
        and (improvement_surcharge is null or improvement_surcharge >= 0)
        and (total_amount is null or total_amount > 0)
        and (payment_type is null or payment_type in (1, 2, 3, 4, 5, 6))

        -- remove outliers
        and passenger_count
            between 1
            and (
                select percentiles[offset(95)]
                from (
                    select approx_quantiles(passenger_count, 100) percentiles
                    from type_casted
                )
            )
        and trip_distance
            between (
                select percentiles[offset(5)]
                from (
                    select approx_quantiles(trip_distance, 100) percentiles
                    from type_casted
                )
            )
            and (
                select percentiles[offset(95)]
                from (
                    select approx_quantiles(trip_distance, 100) percentiles
                    from type_casted
                )
            )
        and fare_amount
            between (
                select percentiles[offset(5)]
                from (
                    select approx_quantiles(fare_amount, 100) percentiles
                    from type_casted
                )
            )
            and (
                select percentiles[offset(95)]
                from (
                    select approx_quantiles(fare_amount, 100) percentiles
                    from type_casted
                )
            )
)

select * from cleaned

-- dbt build --m <model.sql> --var 'is_test_run: false'
