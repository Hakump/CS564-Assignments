
--q3 FINAL ??? maybe < 1999 do not need before removing about 0.7
create index cus_m_k on CUSTOMER(C_MKTSEGMENT, C_CUSTKEY);
create index lin_k_s on LINEITEM(L_ORDERKEY, L_SHIPDATE) where L_SHIPDATE > '1995-03-15' ;
create index ord_k_d_p on ORDERS(O_ORDERKEY,O_ORDERDATE,O_ORDERPRIORITY);

--q4
create index Lin_k_c_r on LINEITEM(L_ORDERKEY, L_SHIPDATE) where l_commitdate < l_receiptdate;
create index ord_k_d_d on ORDERS(O_ORDERKEY, O_ORDERPRIORITY)where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01';

--q2 FINAL
create index price_part on PARTSUPP(PS_PARTKEY, PS_SUPPLYCOST);
create index part_search on PART(P_SIZE, P_TYPE);