
.timer on
EXPLAIN QUERY PLAN
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate < '1993-10-01' --'[DATE]'
	and o_orderdate >= '1993-07-01'--'[DATE]'
	and exists (
		select
			1
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
			)
group by
	o_orderpriority
;