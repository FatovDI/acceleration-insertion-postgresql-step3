SET SCHEMA 'test_insertion';

\set id random(203958073, 203959073)

select paymentdoc0_.id              as id1_4_,
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
from payment_document paymentdoc0_
where (paymentdoc0_.id not in (select at.id from active_transaction as at))
  and paymentdoc0_.id = :id;
