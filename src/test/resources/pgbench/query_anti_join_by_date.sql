SET SCHEMA 'test_insertion';

-- Генерация случайной даты в заданном диапазоне
-- \set date 'date_trunc(''year'', now() - (random() * interval ''2023-12-31'' - ''2020-01-01''::date))::date + random() * interval ''1 year'' - interval ''1 day'''

-- \set dt '2025-01-19'

SELECT paymentdoc0_.id              as id1_4_,
       paymentdoc0_.account_id      as account10_4_,
       paymentdoc0_.amount          as amount2_4_,
       paymentdoc0_.cur             as cur11_4_,
       paymentdoc0_.expense         as expense3_4_,
       paymentdoc0_.order_date      as order_da4_4_,
       paymentdoc0_.order_number    as order_nu5_4_,
       paymentdoc0_.payment_purpose as payment_6_4_,
       paymentdoc0_.prop_10         as prop_7_4_,
       paymentdoc0_.prop_15         as prop_8_4_,
       paymentdoc0_.prop_20         as prop_9_4_
FROM payment_document paymentdoc0_
WHERE NOT EXISTS(SELECT * FROM active_transaction at WHERE at.id = paymentdoc0_.id)
  AND paymentdoc0_.order_date = '2025-01-19';
--   AND paymentdoc0_.order_date = :dt;
