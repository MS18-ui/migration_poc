#!/bin/bash
sqoop import \
  --connect jdbc:mysql://db/sales \
  --username etl --table orders \
  --target-dir /raw/orders \
  --num-mappers 4 --split-by order_id
