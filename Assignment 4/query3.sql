

.timer on
EXPLAIN QUERY PLAN
select
	o_orderkey,
	sum(l_extendedprice*(1-l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING' --'[SEGMENT]'
	and o_orderdate < '1995-03-15' --'[DATE]'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
    and l_shipdate > '1995-03-15' --'[DATE]'
group by
	o_orderkey, --l to o reduce a temp btree
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;