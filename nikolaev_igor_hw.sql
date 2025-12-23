CREATE SCHEMA IF NOT EXISTS memory.dds;

-- Hub Customer
CREATE TABLE IF NOT EXISTS memory.dds.hub_customer (
    hk_customer_h VARBINARY, -- Hash Key
    custkey       BIGINT,    -- Business Key
    load_dt       TIMESTAMP,
    rec_src       VARCHAR
);

-- Hub Order
CREATE TABLE IF NOT EXISTS memory.dds.hub_order (
    hk_order_h    VARBINARY,
    orderkey      BIGINT,
    load_dt       TIMESTAMP,
    rec_src       VARCHAR
);

-- Hub Part
CREATE TABLE IF NOT EXISTS memory.dds.hub_part (
    hk_part_h     VARBINARY,
    partkey       BIGINT,
    load_dt       TIMESTAMP,
    rec_src       VARCHAR
);

-- Hub Supplier
CREATE TABLE IF NOT EXISTS memory.dds.hub_supplier (
    hk_supplier_h VARBINARY,
    suppkey       BIGINT,
    load_dt       TIMESTAMP,
    rec_src       VARCHAR
);


-- Link Order -> Customer
CREATE TABLE IF NOT EXISTS memory.dds.link_order_customer (
    hk_l_ord_cust VARBINARY, 
    hk_order_h    VARBINARY, 
    hk_customer_h VARBINARY, 
    load_dt       TIMESTAMP,
    rec_src       VARCHAR
);

-- Link Lineitem 
CREATE TABLE IF NOT EXISTS memory.dds.link_lineitem (
    hk_l_lineitem VARBINARY,
    hk_order_h    VARBINARY,
    hk_part_h     VARBINARY,
    hk_supplier_h VARBINARY,
    load_dt       TIMESTAMP,
    rec_src       VARCHAR
);

-- Satellite Customer
CREATE TABLE IF NOT EXISTS memory.dds.sat_customer (
    hk_customer_h VARBINARY,
    load_dt       TIMESTAMP,
    rec_src       VARCHAR,
    name          VARCHAR,
    address       VARCHAR,
    phone         VARCHAR,
    mktsegment    VARCHAR,
    hash_diff     VARBINARY
);

-- Satellite Order
CREATE TABLE IF NOT EXISTS memory.dds.sat_order (
    hk_order_h    VARBINARY,
    load_dt       TIMESTAMP,
    rec_src       VARCHAR,
    orderstatus   VARCHAR,
    totalprice    DOUBLE,
    orderdate     DATE,
    orderpriority VARCHAR,
    clerk         VARCHAR,
    hash_diff     VARBINARY
);

-- Satellite Lineitem
CREATE TABLE IF NOT EXISTS memory.dds.sat_lineitem (
    hk_l_lineitem VARBINARY,
    linenumber    INTEGER,  
    load_dt       TIMESTAMP,
    rec_src       VARCHAR,
    quantity      DOUBLE,
    extendedprice DOUBLE,
    discount      DOUBLE,
    tax           DOUBLE,
    returnflag    VARCHAR,
    linestatus    VARCHAR,
    hash_diff     VARBINARY
);

-- Load Hub Customer
INSERT INTO memory.dds.hub_customer
SELECT DISTINCT
    md5(to_utf8(cast(custkey as varchar))) as hk_customer_h,
    custkey,
    current_timestamp as load_dt,
    'TPC-H' as rec_src
FROM tpch.tiny.customer;

-- Load Hub Part
INSERT INTO memory.dds.hub_part
SELECT DISTINCT
    md5(to_utf8(cast(partkey as varchar))),
    partkey,
    current_timestamp,
    'TPC-H'
FROM tpch.tiny.part;

-- Load Hub Supplier
INSERT INTO memory.dds.hub_supplier
SELECT DISTINCT
    md5(to_utf8(cast(suppkey as varchar))),
    suppkey,
    current_timestamp,
    'TPC-H'
FROM tpch.tiny.supplier;

-- Load Sat Customer
INSERT INTO memory.dds.sat_customer
SELECT
    md5(to_utf8(cast(custkey as varchar))) as hk_customer_h,
    current_timestamp as load_dt,
    'TPC-H' as rec_src,
    name,
    address,
    phone,
    mktsegment,
    md5(to_utf8(concat(name, address, phone, mktsegment))) as hash_diff
FROM tpch.tiny.customer;

-- 1. Load Hub Order 
INSERT INTO memory.dds.hub_order
SELECT DISTINCT
    md5(to_utf8(cast(orderkey as varchar))),
    orderkey,
    current_timestamp,
    'TPC-H'
FROM tpch.tiny.orders
WHERE orderdate = DATE '1996-01-02'
  AND md5(to_utf8(cast(orderkey as varchar))) NOT IN (SELECT hk_order_h FROM memory.dds.hub_order);


-- 2. Load Link Order_Customer 
INSERT INTO memory.dds.link_order_customer
SELECT DISTINCT
    md5(to_utf8(concat(cast(orderkey as varchar), cast(custkey as varchar)))),
    md5(to_utf8(cast(orderkey as varchar))) as hk_order_h,
    md5(to_utf8(cast(custkey as varchar))) as hk_customer_h,
    current_timestamp,
    'TPC-H'
FROM tpch.tiny.orders
WHERE orderdate = DATE '1996-01-02';


-- 3. Load Sat Order 
INSERT INTO memory.dds.sat_order
SELECT
    md5(to_utf8(cast(orderkey as varchar))),
    current_timestamp,
    'TPC-H',
    orderstatus,
    totalprice,
    orderdate,
    orderpriority,
    clerk,
    md5(to_utf8(concat(orderstatus, cast(totalprice as varchar), orderpriority))) 
FROM tpch.tiny.orders
WHERE orderdate = DATE '1996-01-02';


-- 4. Load Link Lineitem 
INSERT INTO memory.dds.link_lineitem
SELECT DISTINCT
    md5(to_utf8(concat(
        cast(l.orderkey as varchar),
        cast(l.partkey as varchar),
        cast(l.suppkey as varchar)
    ))),
    md5(to_utf8(cast(l.orderkey as varchar))), 
    md5(to_utf8(cast(l.partkey as varchar))),  
    md5(to_utf8(cast(l.suppkey as varchar))), 
    current_timestamp,
    'TPC-H'
FROM tpch.tiny.lineitem l
JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey
WHERE o.orderdate = DATE '1996-01-02';


-- 5. Load Sat Lineitem 
INSERT INTO memory.dds.sat_lineitem
SELECT
    md5(to_utf8(concat(
        cast(l.orderkey as varchar),
        cast(l.partkey as varchar),
        cast(l.suppkey as varchar)
    ))) as hk_l_lineitem,
    l.linenumber,
    current_timestamp,
    'TPC-H',
    l.quantity,
    l.extendedprice,
    l.discount,
    l.tax,
    l.returnflag,
    l.linestatus,
    md5(to_utf8(concat(
        cast(l.quantity as varchar),
        cast(l.extendedprice as varchar),
        l.linestatus
    )))
FROM tpch.tiny.lineitem l
JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey
WHERE o.orderdate = DATE '1996-01-02';