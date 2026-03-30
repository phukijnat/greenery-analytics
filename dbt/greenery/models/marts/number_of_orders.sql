with

orders as (

		select * from {{ ref('stg_greenery_orders') }}

)

, final as (

		select
				count(distinct order_guid) as order_count

		from orders

)

select * from final