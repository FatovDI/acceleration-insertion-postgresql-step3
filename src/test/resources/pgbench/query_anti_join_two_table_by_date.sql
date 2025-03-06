SET SCHEMA 'test_insertion';

-- Генерация случайной даты в заданном диапазоне
-- \set date 'date_trunc(''year'', now() - (random() * interval ''2023-12-31'' - ''2020-01-01''::date))::date + random() * interval ''1 year'' - interval ''1 day'''

-- \set dt '2025-01-19'

select paymentdoc0_.id              as id1_2_,
       paymentdoc0_.transaction_id  as transact2_2_,
       paymentdoc0_.account_id      as account_9_4_,
       paymentdoc0_.amount          as amount1_4_,
       paymentdoc0_.cur             as cur10_4_,
       paymentdoc0_.expense         as expense2_4_,
       paymentdoc0_.order_date      as order_da3_4_,
       paymentdoc0_.order_number    as order_nu4_4_,
       paymentdoc0_.payment_purpose as payment_5_4_,
       paymentdoc0_.prop_10         as prop_6_4_,
       paymentdoc0_.prop_15         as prop_7_4_,
       paymentdoc0_.prop_20         as prop_8_4_
from payment_document paymentdoc0_
where (NOT EXISTS(SELECT * FROM active_transaction at WHERE at.transaction_id = paymentdoc0_.transaction_id))
  AND paymentdoc0_.order_date = '2025-02-03';
--   AND paymentdoc0_.order_date = :dt;
