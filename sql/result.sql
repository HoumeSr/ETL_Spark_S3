with
    a as (
        select store_id, amount
        from order o
            join user u on o.user_id = u.id
        where
            year (u.created_at) = 2025
    ),
    b as (
        select name, city, sum(amount) as sum_amount
        from a
            join store s on s.id = a.store_id
        group by
            s.city,
            s.name
    ),
    c as (
        select
            city,
            name,
            sum_amount,
            rank() over (
                partition by
                    city
                order by sum_amount desc
            ) as rank_amount
        from b
    )
select
    city,
    name as store_name,
    sum_amount as target_amount
from c
where
    rank_amount <= 3
order by city, rank_amount