with

users as (

		select * from {{ ref('stg_greenery_users') }}

)

, final as (

		select
				count(distinct user_guid) as user_count

		from users

)

select * from final