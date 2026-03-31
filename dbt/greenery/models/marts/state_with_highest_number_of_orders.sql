with

int_orders_addresses_joined as (

		select * from {{ ref('int_orders_addresses_joined') }}

)

, final as (

    select
        state
        , count(order_guid) as number_of_orders

    from int_orders_addresses_joined
    group by state
    order by 2 desc
    limit 1

)

select * from final